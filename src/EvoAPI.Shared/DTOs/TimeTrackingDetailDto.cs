namespace EvoAPI.Shared.DTOs;

public class TimeTrackingDetailDto
{
    public int ttd_id { get; set; }
    public DateTime ttd_insertdatetime { get; set; }
    public DateTime? ttd_modifieddatetime { get; set; }
    public int ttt_id { get; set; }
    public int u_id { get; set; }
    public int? wo_id { get; set; }
    public DateTime? wo_startdatetime { get; set; }
    public DateTime? wo_enddatetime { get; set; }
    public decimal? ttd_lat { get; set; }
    public decimal? ttd_lon { get; set; }
    public decimal? ttd_distanceinmiles { get; set; }
    public int? ttd_traveltimeinminutes { get; set; }
}

public class CreateTimeTrackingDetailRequest
{
    public int u_id { get; set; }
    public int ttt_id { get; set; }
    public int? wo_id { get; set; }
    public decimal? ttd_lat_browser { get; set; }
    public decimal? ttd_lon_browser { get; set; }
    public string? ttd_type { get; set; }
}