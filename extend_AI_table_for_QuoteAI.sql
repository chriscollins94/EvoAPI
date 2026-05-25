-- Extends the existing dbo.AI logging table for the new generic AI use cases
-- (Quote AI Agent today, others later). All adds are nullable so existing
-- INSERTs (e.g. EvoWS GetFormatTextForClient) continue to work unchanged.
--
-- Also widens ai_input / ai_output / ai_prompt to VARCHAR(MAX) so file
-- contents and JSON-schema prompts don't get truncated. VARCHAR widening
-- is backwards-compatible with existing readers.

USE [Evo]; -- adjust DB name if different in your environment
GO

-- New nullable columns ------------------------------------------------------
IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE Name = N'ai_filename' AND Object_ID = Object_ID(N'dbo.AI'))
    ALTER TABLE dbo.AI ADD ai_filename varchar(255) NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE Name = N'ai_filesize' AND Object_ID = Object_ID(N'dbo.AI'))
    ALTER TABLE dbo.AI ADD ai_filesize int NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE Name = N'ai_model' AND Object_ID = Object_ID(N'dbo.AI'))
    ALTER TABLE dbo.AI ADD ai_model varchar(100) NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.columns WHERE Name = N'ai_error' AND Object_ID = Object_ID(N'dbo.AI'))
    ALTER TABLE dbo.AI ADD ai_error varchar(2000) NULL;
GO

-- Widen narrow text columns -------------------------------------------------
ALTER TABLE dbo.AI ALTER COLUMN ai_input  varchar(MAX) NULL;
GO
ALTER TABLE dbo.AI ALTER COLUMN ai_output varchar(MAX) NULL;
GO
ALTER TABLE dbo.AI ALTER COLUMN ai_prompt varchar(MAX) NULL;
GO

-- Seed the QuoteAI ConfigSetting --------------------------------------------
-- One JSON blob per use case. Admins edit cs_value here to retune the model,
-- prompts, endpoint, etc. without redeploying.
--
-- IMPORTANT: o_id = 1 is required on every ConfigSetting INSERT (column is
-- NOT NULL and not in the DTO).
--
-- Replace the placeholder values (token, endpoint) before going live.

IF NOT EXISTS (
    SELECT 1 FROM dbo.ConfigSetting
    WHERE cs_type = 'AI' AND cs_identifier = 'QuoteAI'
)
BEGIN
    INSERT INTO dbo.ConfigSetting (o_id, cs_type, cs_identifier, cs_value, cs_description, cs_insertdatetime)
    VALUES (
        1,
        'AI',
        'QuoteAI',
        N'{
  "enabled": true,
  "endpoint": "https://api.openai.com/v1/chat/completions",
  "token": "",
  "model": "gpt-5.4-mini",
  "maxTokens": 8192,
  "temperature": 0.2,
  "maxFileSizeMb": 20,
  "allowedFileExtensions": [".pdf", ".docx", ".png", ".jpg", ".jpeg", ".txt"],
  "systemPrompt": "You are an assistant that reads quote requests sent to Evolution Maintenance and extracts a structured quote. Return JSON that matches the supplied schema. If a field is unknown, leave it as an empty string or empty array — do not invent data. Compute line totals as quantity * unitPrice; compute subtotal as the sum of line totals; total = subtotal + tax.",
  "outputSchema": {
    "name": "evolution_maintenance_quote",
    "strict": true,
    "schema": {
      "type": "object",
      "additionalProperties": false,
      "properties": {
        "customerName":    { "type": "string" },
        "customerAddress": { "type": "string" },
        "contactName":     { "type": "string" },
        "contactEmail":    { "type": "string" },
        "contactPhone":    { "type": "string" },
        "quoteDate":       { "type": "string" },
        "quoteNumber":     { "type": "string" },
        "subject":         { "type": "string" },
        "summary":         { "type": "string" },
        "lineItems": {
          "type": "array",
          "items": {
            "type": "object",
            "additionalProperties": false,
            "properties": {
              "description": { "type": "string" },
              "quantity":    { "type": "number" },
              "unitPrice":   { "type": "number" },
              "total":       { "type": "number" }
            },
            "required": ["description","quantity","unitPrice","total"]
          }
        },
        "subtotal":   { "type": "number" },
        "tax":        { "type": "number" },
        "total":      { "type": "number" },
        "terms":      { "type": "string" },
        "validUntil": { "type": "string" },
        "notes":      { "type": "string" }
      },
      "required": [
        "customerName","customerAddress","contactName","contactEmail","contactPhone",
        "quoteDate","quoteNumber","subject","summary",
        "lineItems","subtotal","tax","total","terms","validUntil","notes"
      ]
    }
  }
}',
        'Quote AI Agent configuration (model, prompts, output schema). Used by EvoAPI /EvoApi/ai/quote-pdf.',
        SYSUTCDATETIME()
    );
END
GO
