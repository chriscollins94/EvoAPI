using System;

namespace EvoAPI.Shared.DTOs
{
    public class ServiceItemDto
    {
        public int si_id { get; set; }
        public int o_id { get; set; }
        public int siu_id { get; set; }
        public int t_id { get; set; }
        public int sim_id { get; set; }
        public int? sit_id { get; set; }
        public DateTime si_insertdatetime { get; set; }
        public DateTime? si_modifieddatetime { get; set; }
        public string si_name { get; set; } = string.Empty;
        public string? si_description { get; set; }
        public string? si_partnumber { get; set; }
        public decimal? si_basecost { get; set; }
        public bool si_markupenabled { get; set; }
        public bool si_taxable { get; set; }
        public string? si_quickbooksnumber { get; set; }
        public bool si_active { get; set; }
        public bool si_override { get; set; }
        public string? si_keywords { get; set; }
        public string? si_status { get; set; }
        public string? si_datasource { get; set; }
        public decimal? si_hoursperunit { get; set; }
        
        // Navigation properties
        public string? siu_unit { get; set; }
        public string? sim_manufacturer { get; set; }
        public string? t_trade { get; set; }
        public string? sit_serviceitemtype { get; set; }
        
        // Usage statistics
        public int? UsageCount { get; set; }
    }

    public class ServiceItemUnitDto
    {
        public int siu_id { get; set; }
        public DateTime siu_insertdatetime { get; set; }
        public DateTime? siu_modifieddatetime { get; set; }
        public string siu_unit { get; set; } = string.Empty;
        public string? siu_description { get; set; }
        public int? siu_order { get; set; }
    }

    public class ServiceItemManufacturerDto
    {
        public int sim_id { get; set; }
        public DateTime sim_insertdatetime { get; set; }
        public DateTime? sim_modifieddatetime { get; set; }
        public string sim_manufacturer { get; set; } = string.Empty;
    }

    public class ServiceItemTypeDto
    {
        public int sit_id { get; set; }
        public DateTime sit_insertdatetime { get; set; }
        public DateTime? sit_modifieddatetime { get; set; }
        public string sit_serviceitemtype { get; set; } = string.Empty;
    }

    public class TradeDto
    {
        public int t_id { get; set; }
        public int o_id { get; set; }
        public int? t_id_parent { get; set; }
        public DateTime t_insertdatetime { get; set; }
        public DateTime? t_modifieddatetime { get; set; }
        public string t_trade { get; set; } = string.Empty;
        public string? t_description { get; set; }
        public int? t_nte { get; set; }
        public bool? t_parentonly { get; set; }
        public bool? t_highvolume { get; set; }
    }

    public class CreateServiceItemRequest
    {
        public int o_id { get; set; } = 1;
        public int siu_id { get; set; }
        public int sim_id { get; set; } = 1;
        public int t_id { get; set; }
        public int? sit_id { get; set; }
        public string si_name { get; set; } = string.Empty;
        public string? si_description { get; set; }
        public string? si_keywords { get; set; }
        public string? si_partnumber { get; set; }
        public decimal? si_basecost { get; set; }
        public bool si_markupenabled { get; set; } = true;
        public bool si_taxable { get; set; } = false;
        public string? si_quickbooksnumber { get; set; }
        public bool si_active { get; set; } = true;
        public bool si_override { get; set; } = true;
        public string? si_status { get; set; }
        public string? si_datasource { get; set; }
        public decimal? si_hoursperunit { get; set; }
    }

    public class UpdateServiceItemRequest
    {
        public int si_id { get; set; }
        public int o_id { get; set; }
        public int siu_id { get; set; }
        public int sim_id { get; set; }
        public int t_id { get; set; }
        public int? sit_id { get; set; }
        public string si_name { get; set; } = string.Empty;
        public string? si_description { get; set; }
        public string? si_keywords { get; set; }
        public string? si_partnumber { get; set; }
        public decimal? si_basecost { get; set; }
        public bool si_markupenabled { get; set; }
        public bool si_taxable { get; set; }
        public string? si_quickbooksnumber { get; set; }
        public bool si_active { get; set; }
        public bool si_override { get; set; }
        public string? si_status { get; set; }
        public string? si_datasource { get; set; }
        public decimal? si_hoursperunit { get; set; }
    }
}
