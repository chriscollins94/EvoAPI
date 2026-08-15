namespace EvoAPI.Shared.DTOs
{
    // Lookup-admin shape for the Trades settings page (camelCase via JSON).
    // Distinct from the raw-column TradeDto used by service-item flows.
    public class TradeLookupDto
    {
        public int TradeId { get; set; }
        public string Trade { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public int? Nte { get; set; }
        public int? ParentTradeId { get; set; }
        public string? ParentTradeName { get; set; }
        public bool ParentOnly { get; set; }
        public bool Active { get; set; }
    }
}
