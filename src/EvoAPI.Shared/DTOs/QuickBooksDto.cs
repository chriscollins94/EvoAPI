using System.Text.Json.Serialization;

namespace EvoAPI.Shared.DTOs;

public class QuickBooksServiceRequestRow
{
    public int sr_id { get; set; }
    public string sr_requestnumber { get; set; } = string.Empty;
    public string? sr_quickbooks_docnumber { get; set; }
    public string? sr_quickbooks_synctoken { get; set; }
}

public class QuickBooksInvoiceCheckDto
{
    public int sr_id { get; set; }
    public string sr_requestnumber { get; set; } = string.Empty;
    public string? sr_quickbooks_docnumber { get; set; }
    public string? dbSyncToken { get; set; }
    public string? qbSyncToken { get; set; }
    public string? qbId { get; set; }
    public string? qbCreateTime { get; set; }
    public bool isMismatch { get; set; }
    public int qbInvoiceMatches { get; set; }
    public string? error { get; set; }
    public QuickBooksInvoice? qbInvoice { get; set; }
    public string? qbRawResponse { get; set; }
}

public class UpdateQuickBooksSyncTokenRequest
{
    public string SyncToken { get; set; } = string.Empty;
}

public class QuickBooksInvoiceQueryResponse
{
    [JsonPropertyName("QueryResponse")]
    public QuickBooksQueryResponseInner? QueryResponse { get; set; }
}

public class QuickBooksQueryResponseInner
{
    [JsonPropertyName("Invoice")]
    public List<QuickBooksInvoice>? Invoice { get; set; }

    [JsonPropertyName("startPosition")]
    public int? StartPosition { get; set; }

    [JsonPropertyName("maxResults")]
    public int? MaxResults { get; set; }
}

public class QuickBooksInvoice
{
    [JsonPropertyName("Id")]
    public string? Id { get; set; }

    [JsonPropertyName("SyncToken")]
    public string? SyncToken { get; set; }

    [JsonPropertyName("DocNumber")]
    public string? DocNumber { get; set; }

    [JsonPropertyName("TxnDate")]
    public string? TxnDate { get; set; }

    [JsonPropertyName("DueDate")]
    public string? DueDate { get; set; }

    [JsonPropertyName("TrackingNum")]
    public string? TrackingNum { get; set; }

    [JsonPropertyName("TotalAmt")]
    public decimal? TotalAmt { get; set; }

    [JsonPropertyName("Balance")]
    public decimal? Balance { get; set; }

    [JsonPropertyName("PrintStatus")]
    public string? PrintStatus { get; set; }

    [JsonPropertyName("EmailStatus")]
    public string? EmailStatus { get; set; }

    [JsonPropertyName("MetaData")]
    public QuickBooksInvoiceMetaData? MetaData { get; set; }

    [JsonPropertyName("CustomerRef")]
    public QuickBooksRef? CustomerRef { get; set; }

    [JsonPropertyName("CustomerMemo")]
    public QuickBooksValueWrapper? CustomerMemo { get; set; }

    [JsonPropertyName("BillAddr")]
    public QuickBooksAddress? BillAddr { get; set; }

    [JsonPropertyName("ShipAddr")]
    public QuickBooksAddress? ShipAddr { get; set; }

    [JsonPropertyName("ShipFromAddr")]
    public QuickBooksAddress? ShipFromAddr { get; set; }

    [JsonPropertyName("SalesTermRef")]
    public QuickBooksRef? SalesTermRef { get; set; }

    [JsonPropertyName("CurrencyRef")]
    public QuickBooksRef? CurrencyRef { get; set; }

    [JsonPropertyName("CustomField")]
    public List<QuickBooksCustomField>? CustomField { get; set; }

    [JsonPropertyName("Line")]
    public List<QuickBooksInvoiceLine>? Line { get; set; }

    [JsonPropertyName("LinkedTxn")]
    public List<QuickBooksLinkedTxn>? LinkedTxn { get; set; }
}

public class QuickBooksInvoiceMetaData
{
    [JsonPropertyName("CreateTime")]
    public string? CreateTime { get; set; }

    [JsonPropertyName("LastUpdatedTime")]
    public string? LastUpdatedTime { get; set; }
}

public class QuickBooksRef
{
    [JsonPropertyName("value")]
    public string? Value { get; set; }

    [JsonPropertyName("name")]
    public string? Name { get; set; }
}

public class QuickBooksValueWrapper
{
    [JsonPropertyName("value")]
    public string? Value { get; set; }
}

public class QuickBooksAddress
{
    [JsonPropertyName("Id")]
    public string? Id { get; set; }

    [JsonPropertyName("Line1")]
    public string? Line1 { get; set; }

    [JsonPropertyName("Line2")]
    public string? Line2 { get; set; }

    [JsonPropertyName("Line3")]
    public string? Line3 { get; set; }

    [JsonPropertyName("City")]
    public string? City { get; set; }

    [JsonPropertyName("Country")]
    public string? Country { get; set; }

    [JsonPropertyName("CountrySubDivisionCode")]
    public string? CountrySubDivisionCode { get; set; }

    [JsonPropertyName("PostalCode")]
    public string? PostalCode { get; set; }
}

public class QuickBooksCustomField
{
    [JsonPropertyName("DefinitionId")]
    public string? DefinitionId { get; set; }

    [JsonPropertyName("Name")]
    public string? Name { get; set; }

    [JsonPropertyName("Type")]
    public string? Type { get; set; }

    [JsonPropertyName("StringValue")]
    public string? StringValue { get; set; }
}

public class QuickBooksLinkedTxn
{
    [JsonPropertyName("TxnId")]
    public string? TxnId { get; set; }

    [JsonPropertyName("TxnType")]
    public string? TxnType { get; set; }
}

public class QuickBooksInvoiceLine
{
    [JsonPropertyName("Id")]
    public string? Id { get; set; }

    [JsonPropertyName("LineNum")]
    public int? LineNum { get; set; }

    [JsonPropertyName("Description")]
    public string? Description { get; set; }

    [JsonPropertyName("Amount")]
    public decimal? Amount { get; set; }

    [JsonPropertyName("DetailType")]
    public string? DetailType { get; set; }

    [JsonPropertyName("SalesItemLineDetail")]
    public QuickBooksSalesItemLineDetail? SalesItemLineDetail { get; set; }
}

public class QuickBooksSalesItemLineDetail
{
    [JsonPropertyName("ServiceDate")]
    public string? ServiceDate { get; set; }

    [JsonPropertyName("ItemRef")]
    public QuickBooksRef? ItemRef { get; set; }

    [JsonPropertyName("UnitPrice")]
    public decimal? UnitPrice { get; set; }

    [JsonPropertyName("Qty")]
    public decimal? Qty { get; set; }
}
