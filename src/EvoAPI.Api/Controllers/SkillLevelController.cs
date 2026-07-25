using Microsoft.AspNetCore.Mvc;
using EvoAPI.Core.Interfaces;
using EvoAPI.Shared.DTOs;
using EvoAPI.Shared.Attributes;
using System.Diagnostics;
using System.Data;

namespace EvoAPI.Api.Controllers
{
    /// <summary>
    /// Technician skill-level assignments (xrefSkillLevelTechTrade). Migrated from the
    /// legacy EvoWS GetSkillLevel / GetSkillLevelsByTrade / GetSkillLevelsByTech /
    /// UpdateSkillLevel / UpdateSkillLevelNote endpoints.
    /// </summary>
    [ApiController]
    [Route("EvoApi/skill-levels")]
    [EvoAuthorize]
    public class SkillLevelController : BaseController
    {
        private readonly IDataService _dataService;

        public SkillLevelController(IDataService dataService, IAuditService auditService)
        {
            _dataService = dataService;
            InitializeAuditService(auditService);
        }

        /// <summary>
        /// The SkillLevel lookup - the 0-5 scale plus the KY-cleared tiers.
        /// </summary>
        [HttpGet("levels")]
        [SkillLevelOnly]
        public async Task<ActionResult<ApiResponse<List<SkillLevelDto>>>> GetLevels()
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = @"
                    SELECT sl_id, sl_score, sl_description
                    FROM SkillLevel
                    ORDER BY sl_score, sl_description";

                var result = await _dataService.ExecuteQueryAsync(sql);
                var levels = new List<SkillLevelDto>();

                foreach (DataRow row in result.Rows)
                {
                    var description = row["sl_description"]?.ToString() ?? string.Empty;
                    var score = ConvertToInt(row["sl_score"]);
                    var isKentucky = description.Contains("inc KY", StringComparison.OrdinalIgnoreCase);

                    levels.Add(new SkillLevelDto
                    {
                        SkillLevelId = ConvertToInt(row["sl_id"]),
                        Score = score,
                        Description = description,
                        IsKentucky = isKentucky,
                        DisplayLabel = $"{score}{(isKentucky ? "KY" : "")} - {description}"
                    });
                }

                stopwatch.Stop();
                await LogAuditAsync("GetSkillLevels", new { count = levels.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<List<SkillLevelDto>>
                {
                    Success = true,
                    Message = "Skill levels retrieved successfully",
                    Data = levels,
                    Count = levels.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetSkillLevels", ex);

                return StatusCode(500, new ApiResponse<List<SkillLevelDto>>
                {
                    Success = false,
                    Message = "Failed to retrieve skill levels"
                });
            }
        }

        /// <summary>
        /// Child trades, technicians and their assignments for one parent trade - everything
        /// the "By Trade" grid needs. The legacy page made two calls and joined client-side.
        /// </summary>
        [HttpGet("matrix/{parentTradeId}")]
        [SkillLevelOnly]
        public async Task<ActionResult<ApiResponse<SkillMatrixDto>>> GetMatrix(int parentTradeId, [FromQuery] bool includeInactive = false)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string parentSql = @"SELECT t_id, t_trade FROM trade WHERE t_id = @ParentTradeId";
                var parentTable = await _dataService.ExecuteQueryAsync(parentSql, new Dictionary<string, object>
                {
                    { "@ParentTradeId", parentTradeId }
                });

                if (parentTable.Rows.Count == 0)
                {
                    return NotFound(new ApiResponse<SkillMatrixDto>
                    {
                        Success = false,
                        Message = "Parent trade not found"
                    });
                }

                var matrix = new SkillMatrixDto
                {
                    ParentTradeId = parentTradeId,
                    ParentTrade = parentTable.Rows[0]["t_trade"]?.ToString() ?? string.Empty
                };

                const string childSql = @"
                    SELECT t_id, t_trade
                    FROM trade
                    WHERE t_id_parent = @ParentTradeId
                      AND t_active = 1
                    ORDER BY t_trade";

                var childTable = await _dataService.ExecuteQueryAsync(childSql, new Dictionary<string, object>
                {
                    { "@ParentTradeId", parentTradeId }
                });

                foreach (DataRow row in childTable.Rows)
                {
                    matrix.ChildTrades.Add(new SkillTradeDto
                    {
                        TradeId = ConvertToInt(row["t_id"]),
                        Trade = row["t_trade"]?.ToString() ?? string.Empty
                    });
                }

                // EXISTS rather than a join to xrefUserRole so a duplicate role row can't
                // produce two grid rows for the same technician.
                const string techSql = @"
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
                    AND (@IncludeInactive = 1 OR u.u_active = 1)
                    ORDER BY u.u_lastname, u.u_firstname";

                var techTable = await _dataService.ExecuteQueryAsync(techSql, new Dictionary<string, object>
                {
                    { "@IncludeInactive", includeInactive ? 1 : 0 }
                });

                const string assignmentSql = @"
                    SELECT x.u_id, x.t_id, x.sl_id, x.xsltt_note
                    FROM xrefSkillLevelTechTrade x
                    INNER JOIN trade t ON x.t_id = t.t_id
                    WHERE t.t_id_parent = @ParentTradeId";

                var assignmentTable = await _dataService.ExecuteQueryAsync(assignmentSql, new Dictionary<string, object>
                {
                    { "@ParentTradeId", parentTradeId }
                });

                var assignmentsByUser = new Dictionary<int, List<SkillAssignmentDto>>();
                foreach (DataRow row in assignmentTable.Rows)
                {
                    var userId = ConvertToInt(row["u_id"]);
                    if (!assignmentsByUser.TryGetValue(userId, out var list))
                    {
                        list = new List<SkillAssignmentDto>();
                        assignmentsByUser[userId] = list;
                    }

                    list.Add(new SkillAssignmentDto
                    {
                        TradeId = ConvertToInt(row["t_id"]),
                        SkillLevelId = ConvertToNullableInt(row["sl_id"]),
                        Note = row["xsltt_note"] == DBNull.Value ? null : row["xsltt_note"]?.ToString()
                    });
                }

                foreach (DataRow row in techTable.Rows)
                {
                    var userId = ConvertToInt(row["u_id"]);
                    matrix.Technicians.Add(new SkillMatrixTechnicianDto
                    {
                        UserId = userId,
                        FirstName = row["u_firstname"]?.ToString() ?? string.Empty,
                        LastName = row["u_lastname"]?.ToString() ?? string.Empty,
                        EmployeeNumber = row["u_employeenumber"]?.ToString(),
                        ZoneAcronym = row["z_acronym"]?.ToString(),
                        Active = ConvertToBool(row["u_active"]),
                        Assignments = assignmentsByUser.TryGetValue(userId, out var assignments)
                            ? assignments
                            : new List<SkillAssignmentDto>()
                    });
                }

                stopwatch.Stop();
                await LogAuditAsync("GetSkillMatrix", new
                {
                    parentTradeId,
                    includeInactive,
                    childTrades = matrix.ChildTrades.Count,
                    technicians = matrix.Technicians.Count
                }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<SkillMatrixDto>
                {
                    Success = true,
                    Message = "Skill matrix retrieved successfully",
                    Data = matrix,
                    Count = matrix.Technicians.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetSkillMatrix", ex, new { parentTradeId, includeInactive });

                return StatusCode(500, new ApiResponse<SkillMatrixDto>
                {
                    Success = false,
                    Message = "Failed to retrieve skill matrix"
                });
            }
        }

        /// <summary>
        /// Every trade grouped by parent, with one technician's level and note on each.
        /// </summary>
        [HttpGet("technician/{userId}")]
        [SkillLevelOnly]
        public async Task<ActionResult<ApiResponse<TechnicianSkillsDto>>> GetTechnicianSkills(int userId)
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
                    return NotFound(new ApiResponse<TechnicianSkillsDto>
                    {
                        Success = false,
                        Message = "Technician not found"
                    });
                }

                var dto = new TechnicianSkillsDto
                {
                    UserId = userId,
                    FirstName = userTable.Rows[0]["u_firstname"]?.ToString() ?? string.Empty,
                    LastName = userTable.Rows[0]["u_lastname"]?.ToString() ?? string.Empty,
                    ZoneAcronym = userTable.Rows[0]["z_acronym"]?.ToString()
                };

                // Driving off child trades joined to their parent gives the same grouping the
                // legacy page built client-side, minus parents that have no children.
                const string tradeSql = @"
                    SELECT
                        p.t_id AS parent_id,
                        p.t_trade AS parent_trade,
                        c.t_id,
                        c.t_trade,
                        x.sl_id,
                        x.xsltt_note
                    FROM trade c
                    INNER JOIN trade p ON c.t_id_parent = p.t_id
                    LEFT JOIN xrefSkillLevelTechTrade x
                        ON x.t_id = c.t_id AND x.u_id = @UserId
                    WHERE c.t_active = 1
                    ORDER BY p.t_trade, c.t_trade";

                var tradeTable = await _dataService.ExecuteQueryAsync(tradeSql, new Dictionary<string, object>
                {
                    { "@UserId", userId }
                });

                var groupsById = new Dictionary<int, TechnicianSkillGroupDto>();
                foreach (DataRow row in tradeTable.Rows)
                {
                    var parentId = ConvertToInt(row["parent_id"]);
                    if (!groupsById.TryGetValue(parentId, out var group))
                    {
                        group = new TechnicianSkillGroupDto
                        {
                            ParentTradeId = parentId,
                            ParentTrade = row["parent_trade"]?.ToString() ?? string.Empty
                        };
                        groupsById[parentId] = group;
                        dto.Groups.Add(group);
                    }

                    group.Children.Add(new TechnicianSkillRowDto
                    {
                        TradeId = ConvertToInt(row["t_id"]),
                        Trade = row["t_trade"]?.ToString() ?? string.Empty,
                        SkillLevelId = ConvertToNullableInt(row["sl_id"]),
                        Note = row["xsltt_note"] == DBNull.Value ? null : row["xsltt_note"]?.ToString()
                    });
                }

                stopwatch.Stop();
                await LogAuditAsync("GetTechnicianSkills", new { userId, groups = dto.Groups.Count }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<TechnicianSkillsDto>
                {
                    Success = true,
                    Message = "Technician skills retrieved successfully",
                    Data = dto,
                    Count = dto.Groups.Count
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("GetTechnicianSkills", ex, new { userId });

                return StatusCode(500, new ApiResponse<TechnicianSkillsDto>
                {
                    Success = false,
                    Message = "Failed to retrieve technician skills"
                });
            }
        }

        /// <summary>
        /// Upsert one (technician, trade) cell. SetLevel/SetNote scope the write so saving a
        /// note can't wipe a level and vice versa.
        /// </summary>
        [HttpPut("assignment")]
        [SkillLevelOnly]
        public async Task<ActionResult<ApiResponse<object>>> SaveAssignment([FromBody] UpsertSkillAssignmentRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (request.UserId <= 0 || request.TradeId <= 0)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "A technician and a trade are required"
                    });
                }

                if (!request.SetLevel && !request.SetNote)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Nothing to update - set setLevel and/or setNote"
                    });
                }

                // Clearing a level removes the whole assignment; that goes through DELETE so
                // the caller knows the note goes with it.
                if (request.SetLevel && !request.SkillLevelId.HasValue)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Use DELETE to clear an assignment"
                    });
                }

                // sl_id is the anchor of the row - a note-only write can't create one. The UI
                // keeps the note disabled until a level is set; this is the server-side guard.
                const string sql = @"
                    MERGE xrefSkillLevelTechTrade AS target
                    USING (SELECT @UserId AS u_id, @TradeId AS t_id) AS source
                    ON target.u_id = source.u_id AND target.t_id = source.t_id
                    WHEN MATCHED THEN
                        UPDATE SET
                            sl_id = CASE WHEN @SetLevel = 1 THEN @SkillLevelId ELSE target.sl_id END,
                            xsltt_note = CASE WHEN @SetNote = 1 THEN @Note ELSE target.xsltt_note END
                    WHEN NOT MATCHED AND @SkillLevelId IS NOT NULL THEN
                        INSERT (u_id, t_id, sl_id, xsltt_note)
                        VALUES (source.u_id, source.t_id, @SkillLevelId, @Note);";

                var affected = await _dataService.ExecuteNonQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@UserId", request.UserId },
                    { "@TradeId", request.TradeId },
                    { "@SkillLevelId", request.SkillLevelId.HasValue ? (object)request.SkillLevelId.Value : DBNull.Value },
                    { "@Note", string.IsNullOrWhiteSpace(request.Note) ? (object)DBNull.Value : request.Note!.Trim() },
                    { "@SetLevel", request.SetLevel ? 1 : 0 },
                    { "@SetNote", request.SetNote ? 1 : 0 }
                });

                if (affected == 0)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "Set a skill level for this trade before adding a note"
                    });
                }

                stopwatch.Stop();
                await LogAuditAsync("SaveSkillAssignment", new
                {
                    userId = request.UserId,
                    tradeId = request.TradeId,
                    skillLevelId = request.SkillLevelId,
                    setLevel = request.SetLevel,
                    setNote = request.SetNote,
                    note = request.SetNote ? request.Note : null
                }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Skill assignment saved",
                    Count = affected
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("SaveSkillAssignment", ex, request);

                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to save skill assignment"
                });
            }
        }

        /// <summary>
        /// Clear an assignment. Removes the level and the note together - the legacy app had
        /// no way to undo a mis-click at all.
        /// </summary>
        [HttpDelete("assignment/{userId}/{tradeId}")]
        [SkillLevelOnly]
        public async Task<ActionResult<ApiResponse<object>>> ClearAssignment(int userId, int tradeId)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                const string sql = @"
                    DELETE FROM xrefSkillLevelTechTrade
                    WHERE u_id = @UserId AND t_id = @TradeId";

                var affected = await _dataService.ExecuteNonQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@UserId", userId },
                    { "@TradeId", tradeId }
                });

                stopwatch.Stop();
                await LogAuditAsync("ClearSkillAssignment", new { userId, tradeId, affected }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = "Skill assignment cleared",
                    Count = affected
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("ClearSkillAssignment", ex, new { userId, tradeId });

                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to clear skill assignment"
                });
            }
        }

        /// <summary>
        /// Set every active child trade of a parent to one level for a single technician.
        /// </summary>
        [HttpPost("bulk-set")]
        [SkillLevelOnly]
        public async Task<ActionResult<ApiResponse<object>>> BulkSet([FromBody] BulkSetSkillLevelRequest request)
        {
            var stopwatch = Stopwatch.StartNew();
            try
            {
                if (request.UserId <= 0 || request.ParentTradeId <= 0 || request.SkillLevelId <= 0)
                {
                    return BadRequest(new ApiResponse<object>
                    {
                        Success = false,
                        Message = "A technician, a parent trade and a skill level are required"
                    });
                }

                const string sql = @"
                    MERGE xrefSkillLevelTechTrade AS target
                    USING (
                        SELECT @UserId AS u_id, t.t_id
                        FROM trade t
                        WHERE t.t_id_parent = @ParentTradeId AND t.t_active = 1
                    ) AS source
                    ON target.u_id = source.u_id AND target.t_id = source.t_id
                    WHEN MATCHED AND @Overwrite = 1 THEN
                        UPDATE SET sl_id = @SkillLevelId
                    WHEN NOT MATCHED THEN
                        INSERT (u_id, t_id, sl_id)
                        VALUES (source.u_id, source.t_id, @SkillLevelId);";

                var affected = await _dataService.ExecuteNonQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@UserId", request.UserId },
                    { "@ParentTradeId", request.ParentTradeId },
                    { "@SkillLevelId", request.SkillLevelId },
                    { "@Overwrite", request.OverwriteExisting ? 1 : 0 }
                });

                stopwatch.Stop();
                await LogAuditAsync("BulkSetSkillLevel", new
                {
                    userId = request.UserId,
                    parentTradeId = request.ParentTradeId,
                    skillLevelId = request.SkillLevelId,
                    overwriteExisting = request.OverwriteExisting,
                    affected
                }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = $"Updated {affected} trade(s)",
                    Count = affected
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("BulkSetSkillLevel", ex, request);

                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to apply skill levels"
                });
            }
        }

        /// <summary>
        /// Copy one technician's skill levels onto another. Notes are deliberately not copied -
        /// they are commentary about a specific person.
        /// </summary>
        [HttpPost("copy")]
        [SkillLevelOnly]
        public async Task<ActionResult<ApiResponse<object>>> CopySkills([FromBody] CopySkillsRequest request)
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

                const string sql = @"
                    MERGE xrefSkillLevelTechTrade AS target
                    USING (
                        SELECT @ToUserId AS u_id, src.t_id, src.sl_id
                        FROM xrefSkillLevelTechTrade src
                        INNER JOIN trade t ON src.t_id = t.t_id
                        WHERE src.u_id = @FromUserId
                          AND src.sl_id IS NOT NULL
                          AND t.t_active = 1
                          AND (@ParentTradeId IS NULL OR t.t_id_parent = @ParentTradeId)
                    ) AS source
                    ON target.u_id = source.u_id AND target.t_id = source.t_id
                    WHEN MATCHED AND @Overwrite = 1 THEN
                        UPDATE SET sl_id = source.sl_id
                    WHEN NOT MATCHED THEN
                        INSERT (u_id, t_id, sl_id)
                        VALUES (source.u_id, source.t_id, source.sl_id);";

                var affected = await _dataService.ExecuteNonQueryAsync(sql, new Dictionary<string, object>
                {
                    { "@FromUserId", request.FromUserId },
                    { "@ToUserId", request.ToUserId },
                    { "@ParentTradeId", request.ParentTradeId.HasValue ? (object)request.ParentTradeId.Value : DBNull.Value },
                    { "@Overwrite", request.OverwriteExisting ? 1 : 0 }
                });

                stopwatch.Stop();
                await LogAuditAsync("CopySkills", new
                {
                    fromUserId = request.FromUserId,
                    toUserId = request.ToUserId,
                    parentTradeId = request.ParentTradeId,
                    overwriteExisting = request.OverwriteExisting,
                    affected
                }, stopwatch.Elapsed.TotalSeconds.ToString("0.00"));

                return Ok(new ApiResponse<object>
                {
                    Success = true,
                    Message = $"Copied {affected} skill level(s)",
                    Count = affected
                });
            }
            catch (Exception ex)
            {
                stopwatch.Stop();
                await LogAuditErrorAsync("CopySkills", ex, request);

                return StatusCode(500, new ApiResponse<object>
                {
                    Success = false,
                    Message = "Failed to copy skill levels"
                });
            }
        }

        private static int ConvertToInt(object value)
        {
            if (value == null || value == DBNull.Value)
                return 0;

            if (int.TryParse(value.ToString(), out var result))
                return result;

            return 0;
        }

        private static int? ConvertToNullableInt(object value)
        {
            if (value == null || value == DBNull.Value)
                return null;

            if (int.TryParse(value.ToString(), out var result))
                return result;

            return null;
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
