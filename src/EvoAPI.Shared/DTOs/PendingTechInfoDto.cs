namespace EvoAPI.Shared.DTOs;

public class PendingTechInfoDto
{
    public int sr_id { get; set; }
    public int xwou_id { get; set; }
    public string sr_requestnumber { get; set; } = string.Empty;
    public string u_firstname { get; set; } = string.Empty;
    public string u_lastname { get; set; } = string.Empty;
    public string wo_insertdatetime { get; set; } = string.Empty;
    public string t_trade { get; set; } = string.Empty;
    public string c_name { get; set; } = string.Empty;
    public string wo_startdatetime { get; set; } = string.Empty;
    
    // Computed properties for frontend compatibility
    public string Company => c_name;
    public string RequestNumber => sr_requestnumber;
    public string Trade => t_trade;
    public string TechFirstName => u_firstname;
    public string TechLastName => u_lastname;
    public string TechName => $"{u_firstname} {u_lastname}".Trim();
    public string InsertDateTime => wo_insertdatetime;
    public string StartDateTime => wo_startdatetime;
    public string startDate => wo_startdatetime; // Add this for frontend compatibility
}
