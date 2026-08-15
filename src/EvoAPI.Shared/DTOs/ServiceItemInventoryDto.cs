using System;
using System.Collections.Generic;

namespace EvoAPI.Shared.DTOs
{
    /// <summary>
    /// One stock row: a service item at a specific facility / rack / shelf / bin.
    /// </summary>
    public class ServiceItemInventoryDto
    {
        public int sii_id { get; set; }
        public int si_id { get; set; }
        public int sif_id { get; set; }
        public int sir_id { get; set; }
        public int? sim_id { get; set; }

        // Service item detail (joined)
        public string si_name { get; set; } = string.Empty;
        public string? si_description { get; set; }
        public string? si_partnumber { get; set; }
        public string? si_keywords { get; set; }
        public decimal? si_basecost { get; set; }
        public string? si_datasource { get; set; }
        public decimal? si_hoursperunit { get; set; }
        public string? si_status { get; set; }
        public string? sim_manufacturer { get; set; }
        public string? siu_unit { get; set; }

        // Location (joined)
        public string? sif_facility { get; set; }
        public string? sir_rack { get; set; }
        public int? sii_shelf { get; set; }
        public int? sii_bin { get; set; }

        // Counts
        public int sii_countavailable { get; set; }
        public int sii_countallocated { get; set; }

        public DateTime sii_insertdatetime { get; set; }
        public DateTime? sii_modifieddatetime { get; set; }

        // Extended value - the legacy page computed these client-side on every digest.
        public decimal ValueAvailable => (si_basecost ?? 0m) * sii_countavailable;
        public decimal ValueAllocated => (si_basecost ?? 0m) * sii_countallocated;
    }

    /// <summary>
    /// Aggregate totals for whatever filter the caller applied. Returned alongside
    /// the (possibly truncated) row list so the header can show real numbers.
    /// </summary>
    public class ServiceItemInventorySummaryDto
    {
        public int RowCount { get; set; }
        public int DistinctItemCount { get; set; }
        public int TotalAvailable { get; set; }
        public int TotalAllocated { get; set; }
        public decimal TotalValueAvailable { get; set; }
        public decimal TotalValueAllocated { get; set; }
        public int OutOfStockCount { get; set; }
    }

    /// <summary>
    /// Row list plus totals plus how many rows actually matched, so the UI can say
    /// "showing 250 of 900" instead of silently truncating like the legacy TOP 250 did.
    /// </summary>
    public class ServiceItemInventoryListDto
    {
        public List<ServiceItemInventoryDto> Items { get; set; } = new();
        public ServiceItemInventorySummaryDto Summary { get; set; } = new();
        public bool Truncated { get; set; }
    }

    public class CreateServiceItemInventoryRequest
    {
        public int si_id { get; set; }
        public int sif_id { get; set; }
        public int sir_id { get; set; }
        public int? sii_shelf { get; set; }
        public int? sii_bin { get; set; }
        public int sii_countavailable { get; set; }
        public int sii_countallocated { get; set; }
    }

    public class UpdateServiceItemInventoryRequest
    {
        public int sii_id { get; set; }
        public int sif_id { get; set; }
        public int sir_id { get; set; }
        public int? sii_shelf { get; set; }
        public int? sii_bin { get; set; }
        public int sii_countavailable { get; set; }
        public int sii_countallocated { get; set; }
        /// <summary>Optional note explaining the change; stored on the movement history row.</summary>
        public string? Reason { get; set; }
    }

    /// <summary>
    /// Move stock from available to allocated when a part leaves the facility.
    /// Unlike the legacy EvoWS DecrementInventory this targets a single row
    /// (or a single facility) instead of every row sharing the si_id.
    /// </summary>
    public class InventoryDecrementRequest
    {
        public int si_id { get; set; }
        public int Quantity { get; set; }
        /// <summary>Specific stock row to draw from. Preferred.</summary>
        public int? sii_id { get; set; }
        /// <summary>Facility to draw from when sii_id is unknown.</summary>
        public int? sif_id { get; set; }
        public string? Reason { get; set; }
    }

    public class InventoryDecrementResultDto
    {
        public int Requested { get; set; }
        /// <summary>Units actually moved from available to allocated.</summary>
        public int Decremented { get; set; }
        /// <summary>Requested minus decremented. Non-zero means the shelf came up short.</summary>
        public int Shortfall { get; set; }
        public bool FullyFulfilled => Shortfall == 0;
        public List<ServiceItemInventoryDto> AffectedRows { get; set; } = new();
    }

    public class ServiceItemInventoryTransactionDto
    {
        public int siit_id { get; set; }
        public int? sii_id { get; set; }
        public int si_id { get; set; }
        public int? sif_id { get; set; }
        public int? sir_id { get; set; }
        public string siit_type { get; set; } = string.Empty;
        public int siit_availabledelta { get; set; }
        public int siit_allocateddelta { get; set; }
        public int? siit_availableafter { get; set; }
        public int? siit_allocatedafter { get; set; }
        public string? siit_reason { get; set; }
        public int? u_id { get; set; }
        public string? siit_username { get; set; }
        public DateTime siit_insertdatetime { get; set; }

        // Joined for display
        public string? si_name { get; set; }
        public string? sif_facility { get; set; }
        public string? sir_rack { get; set; }
    }
}
