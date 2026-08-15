using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Attributes;
using System.Diagnostics;
using System.Data;
using System.Text;

namespace EvoAPI.Api.Controllers
{
    /// <summary>
    /// Technician weekly availability (UserAvailability). Migrated from the legacy EvoWS
    /// GetUserAvailability / UpdateUserAvailability endpoints and the Angular
    /// #/techavailability page.
    ///
    /// Two things the legacy implementation could not do, and this can: clear a slot, and
    /// write more than one slot per round-trip. The legacy page fired one POST per dropdown
    /// change and had no delete path at all, so a mis-click was permanent.
    ///
    /// Day numbering is the legacy app's own - 1=Monday..7=Sunday, NOT SQL Server's DATEPART
    /// default. Hours are 0-23 wall-clock Central time.
    /// </summary>
    [ApiController]
    [Route("EvoApi/tech-availability")]
    [EvoAuthorize]
    public class UserAvailabilityController : BaseController
    {
        private readonly IDataService _dataService;

        // 7 days x 24 hours. A single request can rewrite a whole week but no more, which
        // also keeps the generated MERGE well under SQL Server's 2100-parameter ceiling.
        private const int MaxSlotsPerRequest = 168;

        // Technicians, with zone. EXISTS rather than a join to xrefUserRole so a duplicate
        // role row cannot produce two rows for the same person.
        private const string TechnicianSelect = @"
            SELECT
                u.u_id,
                u.u_firstname,
                u.u_lastname,
                u.u_employeenumber,
                u.u_active,
                z.z_acronym
            FROM [user] u
            LEFT JOIN Zone z ON u.z_id = z.z_id
            WHERE EXISTS (
                SELECT 1
                FROM xrefUserRole x
                INNER JOIN role r ON r.r_id = x.r_id
                WHERE x.u_id = u.u_id AND r.r_role = 'Technician'
            )
            AND (@IncludeInactive = 1 OR u.u_active = 1)";

        public UserAvailabilityController(IDataService dataService, IAuditService auditService)
        {
            _dataService = dataService;
            InitializeAuditService(auditService);
        }

        /// <summary>
        /// The UserAvailabilityStatus lookup. The legacy pages hardcoded this list in three
        /// separate files; reading the table keeps them from drifting apart again.
        /// </summary>
        [HttpGet("statuses")]
        [SkillLevelOnly]
        public async Task<ActionResult<ApiResponse<List<AvailabilityStatusDto>>>> GetStatuses()
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var statuses = await LoadStatusesAsync();

                stopwatch.Stop();
                await LogAuditAsync("GetAvailabilityStatuses", new { count = statuses.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<List<AvailabilityStatusDto>>
                {
                    Success = true,
                    Message = "Availability statuses retrieved successfully",
                    Data = statuses,
                    Count = statuses.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetAvailabilityStatuses", ex);

                return StatusCode(500, new ApiResponse<List<AvailabilityStatusDto>>
                {
                    Success = false,
                    Message = "Failed to retrieve availability statuses"
                });
            }
        }

        /// <summary>
        /// The technician picker, with a set-slot count per person so it is obvious at a
        /// glance who has a week filled in.
        /// </summary>
        [HttpGet("technicians")]
        [SkillLevelOnly]
        public async Task<ActionResult<ApiResponse<List<AvailabilityTechnicianDto>>>> GetTechnicians([FromQuery] bool includeInactive = false)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var sql = TechnicianSelect + @"
                    ORDER BY u.u_lastname, u.u_firstname";

                var techTable = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@IncludeInactive", includeInactive ? 1 : 0 }
                });

                const string countSql = @"
                    SELECT u_id, COUNT(*) AS slot_count
                    FROM UserAvailability
                    GROUP BY u_id";

                var countTable = await _dataService.ExecuteQueryAsync(countSql);
                var countsByUser = new Dictionary<int, int>();
                foreach (DataRow row in countTable.Rows)
                {
                    countsByUser[ConvertToInt(row["u_id"])] = ConvertToInt(row["slot_count"]);
                }

                var technicians = new List<AvailabilityTechnicianDto>();
                foreach (DataRow row in techTable.Rows)
                {
                    var userId = ConvertToInt(row["u_id"]);
                    technicians.Add(new AvailabilityTechnicianDto
                    {
                        UserId = userId,
                        FirstName = row["u_firstname"]?.ToString() ?? string.Empty,
                        LastName = row["u_lastname"]?.ToString() ?? string.Empty,
                        EmployeeNumber = row["u_employeenumber"]?.ToString(),
                        ZoneAcronym = row["z_acronym"]?.ToString(),
                        Active = ConvertToBool(row["u_active"]),
                        SlotCount = countsByUser.TryGetValue(userId, out var count) ? count : 0
                    });
                }

                stopwatch.Stop();
                await LogAuditAsync("GetAvailabilityTechnicians", new { includeInactive, count = technicians.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<List<AvailabilityTechnicianDto>>
                {
                    Success = true,
                    Message = "Technicians retrieved successfully",
                    Data = technicians,
                    Count = technicians.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetAvailabilityTechnicians", ex, new { includeInactive });

                return StatusCode(500, new ApiResponse<List<AvailabilityTechnicianDto>>
                {
                    Success = false,
                    Message = "Failed to retrieve technicians"
                });
            }
        }

        /// <summary>
        /// One technician's week. Slots are sparse - only cells that have been set come back.
        /// </summary>
        [HttpGet("{userId:int}")]
        [SkillLevelOnly]
        public async Task<ActionResult<ApiResponse<TechnicianAvailabilityDto>>> GetAvailability(int userId)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string userSql = @"
                    SELECT u.u_id, u.u_firstname, u.u_lastname, z.z_acronym
                    FROM [user] u
                    LEFT JOIN Zone z ON u.z_id = z.z_id
                    WHERE u.u_id = @UserId";

                var userTable = await _dataService.ExecuteQueryAsync(userSql, new Dictionary<string, object>
                {
                    { "@UserId", userId }
                });

                if (userTable.Rows.Count == 0)
                {
                    return NotFound(new ApiResponse<TechnicianAvailabilityDto>
                    {
                        Success = false,
                        Message = "Technician not found"
                    });
                }

                var dto = new TechnicianAvailabilityDto
                {
                    UserId = userId,
                    FirstName = userTable.Rows[0]["u_firstname"]?.ToString() ?? string.Empty,
                    LastName = userTable.Rows[0]["u_lastname"]?.ToString() ?? string.Empty,
                    ZoneAcronym = userTable.Rows[0]["z_acronym"]?.ToString()
                };

                const string slotSql = @"
                    SELECT ua_dayofweek, ua_hourofday, uas_id
                    FROM UserAvailability
                    WHERE u_id = @UserId
                    ORDER BY ua_dayofweek, ua_hourofday";

                var slotTable = await _dataService.ExecuteQueryAsync(slotSql, new Dictionary<string, object>
                {
                    { "@UserId", userId }
                });

                foreach (DataRow row in slotTable.Rows)
                {
                    dto.Slots.Add(new AvailabilitySlotDto
                    {
                        DayOfWeek = ConvertToInt(row["ua_dayofweek"]),
                        HourOfDay = ConvertToInt(row["ua_hourofday"]),
                        StatusId = ConvertToInt(row["uas_id"])
                    });
                }

                stopwatch.Stop();
                await LogAuditAsync("GetTechnicianAvailability", new { userId, slots = dto.Slots.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<TechnicianAvailabilityDto>
                {
                    Success = true,
                    Message = "Availability retrieved successfully",
                    Data = dto,
                    Count = dto.Slots.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetTechnicianAvailability", ex, new { userId });

                return StatusCode(500, new ApiResponse<TechnicianAvailabilityDto>
                {
                    Success = false,
                    Message = "Failed to retrieve availability"
                });
            }
        }

        /// <summary>
        /// Write a batch of slots for one technician. A null statusId clears that cell -
        /// the operation the legacy page had no way to perform.
        /// </summary>
        [HttpPut("slots")]
        [SkillLevelOnly]
        public async Task<ActionResult<ApiResponse<object>>> SaveSlots([FromBody] SaveAvailabilityRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (request.UserId <= 0)
                {
                    return BadRequest(new ApiResponse<object> { Success = false, Message = "A technician is required" });
                }

                if (request.Slots == null || request.Slots.Count == 0)
                {
                    return BadRequest(new ApiResponse<object> { Success = false, Message = "No slots supplied" });
                }

                if (request.Slots.Count > MaxSlotsPerRequest)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = $"A request can carry at most {MaxSlotsPerRequest} slots (one full week)"
                    });
                }

                // Last write wins per cell. MERGE raises "attempted to update or delete the
                // same row more than once" if the source has duplicate keys, and a fast
                // painting drag can easily emit the same cell twice.
                var slots = DedupeSlots(request.Slots);

                var validationError = ValidateSlotRanges(slots);
                if (validationError != null)
                {
                    return BadRequest(new ApiResponse<object> { Success = false, Message = validationError });
                }

                var validStatusIds = (await LoadStatusesAsync()).Select(s => s.StatusId).ToHashSet();
                var unknown = slots
                    .Where(s => s.StatusId.HasValue && !validStatusIds.Contains(s.StatusId.Value))
                    .Select(s => s.StatusId!.Value)
                    .Distinct()
                    .ToList();

                if (unknown.Count > 0)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = $"Unknown availability status: {string.Join(", ", unknown)}"
                    });
                }

                var parameters = new Dictionary<string, object> { { "@UserId", request.UserId } };
                var values = new StringBuilder();

                for (var i = 0; i < slots.Count; i++)
                {
                    if (i > 0) values.Append(",\n                        ");
                    values.Append($"(CAST(@d{i} AS tinyint), CAST(@h{i} AS tinyint), CAST(@s{i} AS int))");

                    parameters[$"@d{i}"] = slots[i].DayOfWeek;
                    parameters[$"@h{i}"] = slots[i].HourOfDay;
                    parameters[$"@s{i}"] = slots[i].StatusId.HasValue ? (object)slots[i].StatusId!.Value : DBNull.Value;
                }

                // One MERGE covers set, change and clear. ua_insertdatetime is left to its
                // column default, exactly as the legacy INSERT did.
                var sql = $@"
                    MERGE UserAvailability AS target
                    USING (VALUES
                        {values}
                    ) AS source (ua_dayofweek, ua_hourofday, uas_id)
                    ON  target.u_id = @UserId
                    AND target.ua_dayofweek = source.ua_dayofweek
                    AND target.ua_hourofday = source.ua_hourofday
                    WHEN MATCHED AND source.uas_id IS NULL THEN
                        DELETE
                    WHEN MATCHED AND source.uas_id <> target.uas_id THEN
                        UPDATE SET uas_id = source.uas_id, ua_modifieddatetime = SYSDATETIME()
                    WHEN NOT MATCHED BY TARGET AND source.uas_id IS NOT NULL THEN
                        INSERT (u_id, ua_dayofweek, ua_hourofday, uas_id)
                        VALUES (@UserId, source.ua_dayofweek, source.ua_hourofday, source.uas_id);";

                var affected = await _dataService.ExecuteNonQueryAsync(sql, parameters);

                stopwatch.Stop();
                await LogAuditAsync("SaveTechnicianAvailability", new
                {
                    userId = request.UserId,
                    submitted = slots.Count,
                    cleared = slots.Count(s => !s.StatusId.HasValue),
                    affected
                }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Availability saved",
                    Count = affected
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("SaveTechnicianAvailability", ex, new { request.UserId, slots = request.Slots?.Count ?? 0 });

                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to save availability"
                });
            }
        }

        /// <summary>
        /// Clear specific slots, or the technician's whole week when no slots are supplied.
        /// A POST rather than a DELETE so the slot list can travel in a body without
        /// depending on DELETE-with-payload support in the client.
        /// </summary>
        [HttpPost("slots/clear")]
        [SkillLevelOnly]
        public async Task<ActionResult<ApiResponse<object>>> ClearSlots([FromBody] ClearAvailabilityRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (request.UserId <= 0)
                {
                    return BadRequest(new ApiResponse<object> { Success = false, Message = "A technician is required" });
                }

                int affected;
                var wholeWeek = request.Slots == null || request.Slots.Count == 0;

                if (wholeWeek)
                {
                    const string sql = @"DELETE FROM UserAvailability WHERE u_id = @UserId";
                    affected = await _dataService.ExecuteNonQueryAsync(sql, new Dictionary<string, object>
                    {
                        { "@UserId", request.UserId }
                    });
                }
                else
                {
                    if (request.Slots!.Count > MaxSlotsPerRequest)
                    {
                        return BadRequest(new ApiResponse<object>
                        {
                            Success = false,
                            Message = $"A request can carry at most {MaxSlotsPerRequest} slots (one full week)"
                        });
                    }

                    var slots = DedupeSlots(request.Slots);

                    var validationError = ValidateSlotRanges(slots);
                    if (validationError != null)
                    {
                        return BadRequest(new ApiResponse<object> { Success = false, Message = validationError });
                    }

                    var parameters = new Dictionary<string, object> { { "@UserId", request.UserId } };
                    var pairs = new StringBuilder();

                    for (var i = 0; i < slots.Count; i++)
                    {
                        if (i > 0) pairs.Append(",\n                        ");
                        pairs.Append($"(CAST(@d{i} AS tinyint), CAST(@h{i} AS tinyint))");

                        parameters[$"@d{i}"] = slots[i].DayOfWeek;
                        parameters[$"@h{i}"] = slots[i].HourOfDay;
                    }

                    var sql = $@"
                        DELETE ua
                        FROM UserAvailability ua
                        INNER JOIN (VALUES
                        {pairs}
                        ) AS source (ua_dayofweek, ua_hourofday)
                            ON ua.ua_dayofweek = source.ua_dayofweek
                           AND ua.ua_hourofday = source.ua_hourofday
                        WHERE ua.u_id = @UserId;";

                    affected = await _dataService.ExecuteNonQueryAsync(sql, parameters);
                }

                stopwatch.Stop();
                await LogAuditAsync("ClearTechnicianAvailability", new
                {
                    userId = request.UserId,
                    wholeWeek,
                    requested = request.Slots?.Count ?? 0,
                    affected
                }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = wholeWeek ? "Week cleared" : $"Cleared {affected} slot(s)",
                    Count = affected
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("ClearTechnicianAvailability", ex, new { request.UserId });

                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to clear availability"
                });
            }
        }

        /// <summary>
        /// Copy one technician's week onto another. Overwrite replaces the target's week
        /// outright; without it only cells the target has not set are filled in.
        /// </summary>
        [HttpPost("copy")]
        [SkillLevelOnly]
        public async Task<ActionResult<ApiResponse<object>>> Copy([FromBody] CopyAvailabilityRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (request.FromUserId <= 0 || request.ToUserId <= 0)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "A source and a target technician are required"
                    });
                }

                if (request.FromUserId == request.ToUserId)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Source and target technician must be different"
                    });
                }

                // With Overwrite the target's week is dropped first, so the MERGE only ever
                // inserts and the result is an exact mirror of the source.
                const string sql = @"
                    IF @Overwrite = 1
                        DELETE FROM UserAvailability WHERE u_id = @ToUserId;

                    MERGE UserAvailability AS target
                    USING (
                        SELECT ua_dayofweek, ua_hourofday, uas_id
                        FROM UserAvailability
                        WHERE u_id = @FromUserId
                    ) AS source
                    ON  target.u_id = @ToUserId
                    AND target.ua_dayofweek = source.ua_dayofweek
                    AND target.ua_hourofday = source.ua_hourofday
                    WHEN NOT MATCHED BY TARGET THEN
                        INSERT (u_id, ua_dayofweek, ua_hourofday, uas_id)
                        VALUES (@ToUserId, source.ua_dayofweek, source.ua_hourofday, source.uas_id);";

                var affected = await _dataService.ExecuteNonQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@FromUserId", request.FromUserId },
                    { "@ToUserId", request.ToUserId },
                    { "@Overwrite", request.Overwrite ? 1 : 0 }
                });

                stopwatch.Stop();
                await LogAuditAsync("CopyTechnicianAvailability", new
                {
                    fromUserId = request.FromUserId,
                    toUserId = request.ToUserId,
                    overwrite = request.Overwrite,
                    affected
                }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Availability copied",
                    Count = affected
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("CopyTechnicianAvailability", ex, request);

                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to copy availability"
                });
            }
        }

        /// <summary>
        /// Every set slot across all technicians, grouped by cell. One request instead of
        /// one per technician - the page pivots this into the day x hour coverage grid.
        /// </summary>
        [HttpGet("coverage")]
        [SkillLevelOnly]
        public async Task<ActionResult<ApiResponse<AvailabilityCoverageDto>>> GetCoverage([FromQuery] bool includeInactive = false)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                var sql = $@"
                    WITH techs AS (
                        {TechnicianSelect}
                    )
                    SELECT
                        ua.ua_dayofweek,
                        ua.ua_hourofday,
                        ua.uas_id,
                        t.u_id,
                        t.u_firstname,
                        t.u_lastname,
                        t.z_acronym
                    FROM UserAvailability ua
                    INNER JOIN techs t ON t.u_id = ua.u_id
                    ORDER BY ua.ua_dayofweek, ua.ua_hourofday, t.u_lastname, t.u_firstname";

                var table = await _dataService.ExecuteQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@IncludeInactive", includeInactive ? 1 : 0 }
                });

                var dto = new AvailabilityCoverageDto();
                var cellsByKey = new Dictionary<int, CoverageCellDto>();
                var seenTechnicians = new HashSet<int>();

                foreach (DataRow row in table.Rows)
                {
                    var day = ConvertToInt(row["ua_dayofweek"]);
                    var hour = ConvertToInt(row["ua_hourofday"]);
                    var key = day * 100 + hour;

                    if (!cellsByKey.TryGetValue(key, out var cell))
                    {
                        cell = new CoverageCellDto { DayOfWeek = day, HourOfDay = hour };
                        cellsByKey[key] = cell;
                        dto.Cells.Add(cell);
                    }

                    var userId = ConvertToInt(row["u_id"]);
                    seenTechnicians.Add(userId);

                    cell.Technicians.Add(new CoverageTechnicianDto
                    {
                        UserId = userId,
                        FirstName = row["u_firstname"]?.ToString() ?? string.Empty,
                        LastName = row["u_lastname"]?.ToString() ?? string.Empty,
                        ZoneAcronym = row["z_acronym"]?.ToString(),
                        StatusId = ConvertToInt(row["uas_id"])
                    });
                }

                dto.TechnicianCount = seenTechnicians.Count;

                stopwatch.Stop();
                await LogAuditAsync("GetAvailabilityCoverage", new
                {
                    includeInactive,
                    cells = dto.Cells.Count,
                    technicians = dto.TechnicianCount
                }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<AvailabilityCoverageDto>
                {
                    Success = true,
                    Message = "Coverage retrieved successfully",
                    Data = dto,
                    Count = dto.Cells.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetAvailabilityCoverage", ex, new { includeInactive });

                return StatusCode(500, new ApiResponse<AvailabilityCoverageDto>
                {
                    Success = false,
                    Message = "Failed to retrieve coverage"
                });
            }
        }

        private async Task<List<AvailabilityStatusDto>> LoadStatusesAsync()
        {
            const string sql = @"
                SELECT uas_id, uas_status
                FROM UserAvailabilityStatus
                ORDER BY uas_id";

            var table = await _dataService.ExecuteQueryAsync(sql);
            var statuses = new List<AvailabilityStatusDto>();

            foreach (DataRow row in table.Rows)
            {
                var id = ConvertToInt(row["uas_id"]);
                var label = row["uas_status"]?.ToString();

                statuses.Add(new AvailabilityStatusDto
                {
                    StatusId = id,
                    // uas_status is nullable - never hand the UI a blank chip to render.
                    Status = string.IsNullOrWhiteSpace(label) ? $"Status {id}" : label!
                });
            }

            return statuses;
        }

        /// <summary>
        /// Collapse duplicate (day, hour) keys, keeping the last write for each.
        /// </summary>
        private static List<AvailabilitySlotWrite> DedupeSlots(List<AvailabilitySlotWrite> slots)
        {
            var byKey = new Dictionary<int, AvailabilitySlotWrite>();
            foreach (var slot in slots)
            {
                byKey[slot.DayOfWeek * 100 + slot.HourOfDay] = slot;
            }
            return byKey.Values.ToList();
        }

        /// <summary>
        /// ua_dayofweek and ua_hourofday are tinyint - an out-of-range value would otherwise
        /// surface as an arithmetic overflow rather than a usable message.
        /// </summary>
        private static string? ValidateSlotRanges(List<AvailabilitySlotWrite> slots)
        {
            foreach (var slot in slots)
            {
                if (slot.DayOfWeek < 1 || slot.DayOfWeek > 7)
                    return $"Day of week must be 1-7 (Monday-Sunday); got {slot.DayOfWeek}";

                if (slot.HourOfDay < 0 || slot.HourOfDay > 23)
                    return $"Hour of day must be 0-23; got {slot.HourOfDay}";
            }

            return null;
        }

        private static int ConvertToInt(object value)
        {
            if (value == null || value == DBNull.Value)
                return 0;

            if (int.TryParse(value.ToString(), out var result))
                return result;

            return 0;
        }

        private static bool ConvertToBool(object value)
        {
            if (value == null || value == DBNull.Value)
                return false;

            if (bool.TryParse(value.ToString(), out var result))
                return result;

            if (int.TryParse(value.ToString(), out var intResult))
                return intResult != 0;

            return false;
        }
    }
}
