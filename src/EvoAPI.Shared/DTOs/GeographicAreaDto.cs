namespace EvoAPI.Shared.DTOs
{
    public class ZoneMicroDto
    {
        public int ZoneMicroId { get; set; }
        public string ZoneMicroNumber { get; set; } = string.Empty;
        public string? ZoneMicroDescription { get; set; }
        public int TaxRecordCount { get; set; }
    }

    public class TaxRecordDto
    {
        public int TaxId { get; set; }
        public string TaxZip { get; set; } = string.Empty;
        public string? TaxCity { get; set; }
        public string? TaxCounty { get; set; }
        public string? TaxState { get; set; }
        public int ZoneMicroId { get; set; }
    }
}
