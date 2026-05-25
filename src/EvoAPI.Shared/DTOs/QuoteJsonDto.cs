using System.Text.Json.Serialization;

namespace EvoAPI.Shared.DTOs;

// Shape the AI returns for QuoteAI. Mirrors the json_schema in the QuoteAI
// ConfigSetting so the controller can deserialize the OpenAI Structured Output
// straight into this and hand it to QuotePdfRenderer.
public class QuoteJsonDto
{
    [JsonPropertyName("customerName")]    public string CustomerName    { get; set; } = string.Empty;
    [JsonPropertyName("customerAddress")] public string CustomerAddress { get; set; } = string.Empty;
    [JsonPropertyName("contactName")]     public string ContactName     { get; set; } = string.Empty;
    [JsonPropertyName("contactEmail")]    public string ContactEmail    { get; set; } = string.Empty;
    [JsonPropertyName("contactPhone")]    public string ContactPhone    { get; set; } = string.Empty;
    [JsonPropertyName("quoteDate")]       public string QuoteDate       { get; set; } = string.Empty;
    [JsonPropertyName("quoteNumber")]     public string QuoteNumber     { get; set; } = string.Empty;
    [JsonPropertyName("subject")]         public string Subject         { get; set; } = string.Empty;
    [JsonPropertyName("summary")]         public string Summary         { get; set; } = string.Empty;
    [JsonPropertyName("laborEstimate")]   public QuoteLaborEstimateDto? LaborEstimate { get; set; }
    [JsonPropertyName("safetyMeasures")]  public List<QuoteSafetyMeasureDto> SafetyMeasures { get; set; } = new();
    [JsonPropertyName("serviceItems")]    public List<QuoteServiceItemDto> ServiceItems { get; set; } = new();
    [JsonPropertyName("lineItems")]       public List<QuoteLineItemDto> LineItems { get; set; } = new();
    [JsonPropertyName("subtotal")]        public decimal Subtotal       { get; set; }
    [JsonPropertyName("tax")]             public decimal Tax            { get; set; }
    [JsonPropertyName("total")]           public decimal Total          { get; set; }
    [JsonPropertyName("terms")]           public string Terms           { get; set; } = string.Empty;
    [JsonPropertyName("validUntil")]      public string ValidUntil      { get; set; } = string.Empty;
    [JsonPropertyName("notes")]           public string Notes           { get; set; } = string.Empty;
    [JsonPropertyName("estimatorNotes")]  public string EstimatorNotes  { get; set; } = string.Empty;
    [JsonPropertyName("workInstructions")] public List<QuoteWorkInstructionDto> WorkInstructions { get; set; } = new();
}

public class QuoteLineItemDto
{
    [JsonPropertyName("description")]   public string  Description   { get; set; } = string.Empty;
    [JsonPropertyName("quantity")]      public decimal Quantity      { get; set; }
    [JsonPropertyName("unitPrice")]     public decimal UnitPrice     { get; set; }
    [JsonPropertyName("total")]         public decimal Total         { get; set; }
    // AI-tagged role of this line: "labor" / "material" / "other". Drives
    // whether the server applies markup (material only) or leaves it alone.
    [JsonPropertyName("type")]          public string  Type          { get; set; } = string.Empty;
    // Server-computed percent (whole number, e.g. 25 = 25%). AI must return 0.
    [JsonPropertyName("markupPercent")] public decimal MarkupPercent { get; set; }
}

public class QuoteLaborEstimateDto
{
    [JsonPropertyName("technicianCount")] public int     TechnicianCount { get; set; }
    [JsonPropertyName("hoursOnSite")]     public decimal HoursOnSite     { get; set; }
    [JsonPropertyName("totalLaborHours")] public decimal TotalLaborHours { get; set; }
    [JsonPropertyName("rationale")]       public string  Rationale       { get; set; } = string.Empty;
}

public class QuoteServiceItemDto
{
    [JsonPropertyName("name")]               public string  Name               { get; set; } = string.Empty;
    [JsonPropertyName("purpose")]            public string  Purpose            { get; set; } = string.Empty;
    [JsonPropertyName("estimatedQuantity")]  public decimal EstimatedQuantity  { get; set; }
    [JsonPropertyName("unit")]               public string  Unit               { get; set; } = string.Empty;
    [JsonPropertyName("estimatedUnitCost")]  public decimal EstimatedUnitCost  { get; set; }
    [JsonPropertyName("estimatedTotalCost")] public decimal EstimatedTotalCost { get; set; }
    // Server-computed percent. AI must return 0.
    [JsonPropertyName("markupPercent")]      public decimal MarkupPercent      { get; set; }
}

public class QuoteSafetyMeasureDto
{
    [JsonPropertyName("measure")] public string Measure { get; set; } = string.Empty;
    [JsonPropertyName("details")] public string Details { get; set; } = string.Empty;
}

public class QuoteWorkInstructionDto
{
    [JsonPropertyName("step")]        public int    Step        { get; set; }
    [JsonPropertyName("title")]       public string Title       { get; set; } = string.Empty;
    [JsonPropertyName("details")]     public string Details     { get; set; } = string.Empty;
    [JsonPropertyName("safetyNotes")] public string SafetyNotes { get; set; } = string.Empty;
    [JsonPropertyName("toolsNeeded")] public string ToolsNeeded { get; set; } = string.Empty;
}
