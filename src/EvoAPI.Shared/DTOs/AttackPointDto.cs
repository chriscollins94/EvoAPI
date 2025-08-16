namespace EvoAPI.Shared.DTOs;

public class AttackPointDto
{
    public int sr_id { get; set; }
    public string sr_requestnumber { get; set; } = string.Empty;
    public DateTime sr_insertdatetime { get; set; }
    public decimal? sr_totaldue { get; set; }
    public DateTime? sr_datenextstep { get; set; }
    public string sr_actionablenote { get; set; } = string.Empty;
    public string zone { get; set; } = string.Empty;
    public int? admin_u_id { get; set; }
    public string admin_firstname { get; set; } = string.Empty;
    public string admin_lastname { get; set; } = string.Empty;
    public string cc_name { get; set; } = string.Empty;
    public string c_name { get; set; } = string.Empty;
    public string p_priority { get; set; } = string.Empty;
    public string ss_statussecondary { get; set; } = string.Empty;
    public string t_trade { get; set; } = string.Empty;
    public int hours_since_last_note { get; set; }
    public int days_in_current_status { get; set; }
    public int AttackCallCenter { get; set; }
    public int AttackPriority { get; set; }
    public int AttackStatusSecondary { get; set; }
    public int AttackHoursSinceLastNote { get; set; }
    public int AttackDaysInStatus { get; set; }
    public int AttackPoints { get; set; }
}

public class AttackPointRequest
{
    public int TopCount { get; set; } = 15;
}
