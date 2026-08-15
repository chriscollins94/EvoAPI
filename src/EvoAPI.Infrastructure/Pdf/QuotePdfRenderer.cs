using EvoAPI.Shared.DTOs;
using MigraDoc.DocumentObjectModel;
using MigraDoc.DocumentObjectModel.Tables;
using MigraDoc.Rendering;
using PdfSharp.Fonts;
using PdfSharp.Snippets.Font;

namespace EvoAPI.Infrastructure.Pdf;

// Renders a QuoteJsonDto (the AI's structured output) into a branded PDF
// using MigraDoc + PdfSharp (MIT, no headless-browser dependency).
public class QuotePdfRenderer
{
    private static readonly object _fontInit = new();
    private static bool _fontResolverSet;

    public byte[] Render(QuoteJsonDto quote, SrLaborContextDto? context = null, MarkupConfigDto? markup = null)
    {
        EnsureFontResolver();

        var doc = new Document();
        doc.Info.Title  = "Evolution Maintenance Quote";
        doc.Info.Author = "Evolution Maintenance";

        DefineStyles(doc);
        BuildBody(doc, quote, context, markup);

        var renderer = new PdfDocumentRenderer { Document = doc };
        renderer.RenderDocument();

        using var ms = new MemoryStream();
        renderer.PdfDocument.Save(ms, closeStream: false);
        return ms.ToArray();
    }

    private static void EnsureFontResolver()
    {
        // PdfSharp 6.x on Windows can resolve fonts from the OS font collection.
        // FailsafeFontResolver is included as a sample resolver for cross-platform
        // hosts that don't ship the named font.
        if (_fontResolverSet) return;
        lock (_fontInit)
        {
            if (_fontResolverSet) return;
            GlobalFontSettings.UseWindowsFontsUnderWindows = true;
            if (GlobalFontSettings.FontResolver == null)
                GlobalFontSettings.FontResolver = new FailsafeFontResolver();
            _fontResolverSet = true;
        }
    }

    private static void DefineStyles(Document doc)
    {
        var normal = doc.Styles["Normal"]!;
        normal.Font.Name = "Arial";
        normal.Font.Size = 10;

        var h1 = doc.Styles["Heading1"]!;
        h1.Font.Size = 18;
        h1.Font.Bold = true;
        h1.ParagraphFormat.SpaceAfter = "0.2cm";

        var h2 = doc.Styles["Heading2"]!;
        h2.Font.Size = 12;
        h2.Font.Bold = true;
        h2.ParagraphFormat.SpaceBefore = "0.4cm";
        h2.ParagraphFormat.SpaceAfter  = "0.15cm";
    }

    private static void BuildBody(Document doc, QuoteJsonDto quote, SrLaborContextDto? context, MarkupConfigDto? markup)
    {
        var section = doc.AddSection();
        section.PageSetup.PageFormat = PageFormat.Letter;
        section.PageSetup.LeftMargin   = "2cm";
        section.PageSetup.RightMargin  = "2cm";
        section.PageSetup.TopMargin    = "1.5cm";
        section.PageSetup.BottomMargin = "1.5cm";

        // ---- Header --------------------------------------------------------
        var header = section.AddParagraph("Evolution Maintenance");
        header.Style = "Heading1";

        var sub = section.AddParagraph("Quote");
        sub.Format.Font.Size = 14;
        sub.Format.Font.Bold = true;
        sub.Format.SpaceAfter = "0.4cm";

        // ---- Meta (date) --------------------------------------------------
        // Always today — the office user is generating the quote now.
        var dateLine = section.AddParagraph($"Date: {DateTime.Now:MMMM d, yyyy}");
        dateLine.Format.SpaceAfter = "0.2cm";

        if (!string.IsNullOrWhiteSpace(quote.ValidUntil))
            section.AddParagraph($"Valid Until: {quote.ValidUntil}").Format.SpaceAfter = "0.2cm";

        // ---- Customer block ------------------------------------------------
        // Prefer the customer name the AI extracted from the source document;
        // fall back to the company on the selected SR so "Prepared For" is
        // never blank when we know who the SR is for.
        var customerName = !string.IsNullOrWhiteSpace(quote.CustomerName)
            ? quote.CustomerName
            : context?.Company ?? string.Empty;

        section.AddParagraph("Prepared For").Style = "Heading2";
        section.AddParagraph(Safe(customerName));
        if (!string.IsNullOrWhiteSpace(quote.CustomerAddress))
            section.AddParagraph(quote.CustomerAddress);
        if (!string.IsNullOrWhiteSpace(quote.ContactName))
            section.AddParagraph($"Attn: {quote.ContactName}");
        if (!string.IsNullOrWhiteSpace(quote.ContactEmail))
            section.AddParagraph(quote.ContactEmail);
        if (!string.IsNullOrWhiteSpace(quote.ContactPhone))
            section.AddParagraph(quote.ContactPhone);

        // ---- Subject / summary --------------------------------------------
        if (!string.IsNullOrWhiteSpace(quote.Subject))
        {
            section.AddParagraph("Subject").Style = "Heading2";
            section.AddParagraph(quote.Subject);
        }

        if (!string.IsNullOrWhiteSpace(quote.Summary))
        {
            section.AddParagraph("Summary").Style = "Heading2";
            section.AddParagraph(quote.Summary);
        }

        // ---- Safety measures (only when present) --------------------------
        // Called out early so the reader sees special conditions (traffic
        // control, fall protection, confined-space monitoring, etc.) before
        // the labor and pricing breakdowns. AI returns an empty array when
        // no measures beyond standard PPE are required.
        if (quote.SafetyMeasures.Count > 0)
        {
            section.AddParagraph("Safety Measures").Style = "Heading2";
            foreach (var sm in quote.SafetyMeasures)
            {
                if (string.IsNullOrWhiteSpace(sm.Measure) && string.IsNullOrWhiteSpace(sm.Details))
                    continue;

                var p = section.AddParagraph();
                p.Format.SpaceBefore = "0.1cm";
                p.Format.LeftIndent  = "0.3cm";
                if (!string.IsNullOrWhiteSpace(sm.Measure))
                {
                    var t = p.AddFormattedText("• " + sm.Measure, TextFormat.Bold);
                    t.Color = Colors.DarkRed;
                }
                if (!string.IsNullOrWhiteSpace(sm.Details))
                {
                    if (!string.IsNullOrWhiteSpace(sm.Measure)) p.AddText(" — ");
                    p.AddText(sm.Details);
                }
            }
        }

        // ---- Labor estimate ------------------------------------------------
        if (quote.LaborEstimate != null)
        {
            section.AddParagraph("Labor Estimate").Style = "Heading2";

            var labor = section.AddTable();
            labor.Borders.Visible = false;
            labor.AddColumn("4cm");
            labor.AddColumn("12.5cm");

            AddLabelRow(labor, "Technicians",      quote.LaborEstimate.TechnicianCount.ToString());
            AddLabelRow(labor, "Hours on site",    quote.LaborEstimate.HoursOnSite.ToString("0.##"));
            AddLabelRow(labor, "Total labor hours", quote.LaborEstimate.TotalLaborHours.ToString("0.##"));
            if (!string.IsNullOrWhiteSpace(quote.LaborEstimate.Rationale))
                AddLabelRow(labor, "Rationale", quote.LaborEstimate.Rationale);
        }

        // ---- Service items / materials -------------------------------------
        if (quote.ServiceItems.Count > 0)
        {
            section.AddParagraph("Materials & Service Items").Style = "Heading2";

            var items = section.AddTable();
            items.Borders.Width = 0.5;
            items.Borders.Color = Colors.LightGray;
            items.LeftPadding = 4; items.RightPadding = 4;
            items.TopPadding  = 2; items.BottomPadding = 2;

            items.AddColumn("3.4cm");
            items.AddColumn("4.4cm");
            items.AddColumn("1.2cm").Format.Alignment = ParagraphAlignment.Right;
            items.AddColumn("1.3cm").Format.Alignment = ParagraphAlignment.Left;
            items.AddColumn("2.1cm").Format.Alignment = ParagraphAlignment.Right;
            items.AddColumn("1.4cm").Format.Alignment = ParagraphAlignment.Right;
            items.AddColumn("2.2cm").Format.Alignment = ParagraphAlignment.Right;

            var ih = items.AddRow();
            ih.Shading.Color = Colors.LightGray;
            ih.Format.Font.Bold = true;
            ih.Cells[0].AddParagraph("Item");
            ih.Cells[1].AddParagraph("Purpose");
            ih.Cells[2].AddParagraph("Qty");
            ih.Cells[3].AddParagraph("Unit");
            ih.Cells[4].AddParagraph("Unit Cost");
            ih.Cells[5].AddParagraph("Markup");
            ih.Cells[6].AddParagraph("Total");

            decimal materialsTotal = 0m;
            foreach (var si in quote.ServiceItems)
            {
                var lineTotal = si.EstimatedTotalCost > 0
                    ? si.EstimatedTotalCost
                    : si.EstimatedQuantity * si.EstimatedUnitCost;
                materialsTotal += lineTotal;

                var r = items.AddRow();
                r.Cells[0].AddParagraph(Safe(si.Name));
                r.Cells[1].AddParagraph(Safe(si.Purpose));
                r.Cells[2].AddParagraph(si.EstimatedQuantity.ToString("0.##"));
                r.Cells[3].AddParagraph(Safe(si.Unit));
                r.Cells[4].AddParagraph(si.EstimatedUnitCost.ToString("C2"));
                r.Cells[5].AddParagraph(FormatMarkup(si.MarkupPercent));
                r.Cells[6].AddParagraph(lineTotal.ToString("C2"));
            }

            var totalRow = items.AddRow();
            totalRow.Format.Font.Bold = true;
            totalRow.Cells[0].MergeRight = 5;
            totalRow.Cells[0].Format.Alignment = ParagraphAlignment.Right;
            totalRow.Cells[0].AddParagraph("Materials Subtotal (estimated)");
            totalRow.Cells[6].AddParagraph(materialsTotal.ToString("C2"));
        }

        // ---- Line items table ---------------------------------------------
        section.AddParagraph("Line Items").Style = "Heading2";

        var table = section.AddTable();
        table.Borders.Width = 0.5;
        table.Borders.Color = Colors.LightGray;
        table.LeftPadding = 4;
        table.RightPadding = 4;
        table.TopPadding = 2;
        table.BottomPadding = 2;

        table.AddColumn("6.4cm");
        table.AddColumn("1.4cm").Format.Alignment = ParagraphAlignment.Right;
        table.AddColumn("2.1cm").Format.Alignment = ParagraphAlignment.Right;
        table.AddColumn("1.5cm").Format.Alignment = ParagraphAlignment.Right;
        table.AddColumn("1.8cm").Format.Alignment = ParagraphAlignment.Right;
        table.AddColumn("2.8cm").Format.Alignment = ParagraphAlignment.Right;

        var headerRow = table.AddRow();
        headerRow.Shading.Color = Colors.LightGray;
        headerRow.Format.Font.Bold = true;
        headerRow.Cells[0].AddParagraph("Description");
        headerRow.Cells[1].AddParagraph("Qty");
        headerRow.Cells[2].AddParagraph("Unit Price");
        headerRow.Cells[3].AddParagraph("Markup");
        headerRow.Cells[4].AddParagraph("Tax");
        headerRow.Cells[5].AddParagraph("Total");

        foreach (var line in quote.LineItems)
        {
            var r = table.AddRow();
            r.Cells[0].AddParagraph(Safe(line.Description));
            r.Cells[1].AddParagraph(line.Quantity.ToString("0.##"));
            r.Cells[2].AddParagraph(line.UnitPrice.ToString("C2"));
            r.Cells[3].AddParagraph(FormatMarkup(line.MarkupPercent));
            r.Cells[4].AddParagraph(line.TaxAmount > 0 ? line.TaxAmount.ToString("C2") : "—");
            r.Cells[5].AddParagraph(line.Total.ToString("C2"));
        }

        // Markup-source footer — shows the office where the percentages came
        // from (trade override / tiered ranges / company default / none) so
        // the estimator doesn't have to dig into the config to understand the
        // numbers above. Only rendered when we have markup context to share.
        if (markup != null && !string.IsNullOrWhiteSpace(markup.Summary))
        {
            var footerNote = section.AddParagraph();
            footerNote.Format.SpaceBefore = "0.15cm";
            footerNote.Format.Font.Size   = 8;
            footerNote.Format.Font.Color  = Colors.Gray;
            footerNote.Format.Font.Italic = true;
            footerNote.AddText($"Markup source: {markup.Summary}");
        }

        // ---- Totals --------------------------------------------------------
        var totals = section.AddTable();
        totals.Borders.Visible = false;
        totals.AddColumn("13.5cm").Format.Alignment = ParagraphAlignment.Right;
        totals.AddColumn("3cm").Format.Alignment = ParagraphAlignment.Right;

        AddTotalRow(totals, "Subtotal", quote.Subtotal.ToString("C2"), bold: false);
        if (quote.Tax > 0) AddTotalRow(totals, "Tax", quote.Tax.ToString("C2"), bold: false);
        AddTotalRow(totals, "Total",    quote.Total.ToString("C2"), bold: true);

        // ---- Terms / notes -------------------------------------------------
        if (!string.IsNullOrWhiteSpace(quote.Terms))
        {
            section.AddParagraph("Terms").Style = "Heading2";
            section.AddParagraph(quote.Terms);
        }

        if (!string.IsNullOrWhiteSpace(quote.Notes))
        {
            section.AddParagraph("Notes").Style = "Heading2";
            section.AddParagraph(quote.Notes);
        }

        if (!string.IsNullOrWhiteSpace(quote.EstimatorNotes))
        {
            var estHeader = section.AddParagraph("Estimator Notes (internal)");
            estHeader.Style = "Heading2";
            estHeader.Format.Font.Color = Colors.DarkGray;

            var estBody = section.AddParagraph(quote.EstimatorNotes);
            estBody.Format.Font.Italic = true;
            estBody.Format.Font.Color = Colors.DarkGray;
        }

        // ---- Page 2: Technician Work Instructions -------------------------
        if (quote.WorkInstructions.Count > 0)
        {
            section.AddPageBreak();

            var instHeading = section.AddParagraph("Technician Work Instructions");
            instHeading.Style = "Heading1";

            var instSub = section.AddParagraph("Step-by-step playbook for the on-site crew.");
            instSub.Format.Font.Italic = true;
            instSub.Format.Font.Color = Colors.Gray;
            instSub.Format.SpaceAfter = "0.4cm";

            var orderedSteps = quote.WorkInstructions
                .OrderBy(s => s.Step <= 0 ? int.MaxValue : s.Step)
                .ToList();

            foreach (var step in orderedSteps)
            {
                var stepHeader = section.AddParagraph();
                stepHeader.Format.SpaceBefore = "0.3cm";
                stepHeader.Format.SpaceAfter  = "0.1cm";
                stepHeader.Format.KeepWithNext = true;
                var num = step.Step > 0 ? step.Step.ToString() : "•";
                var ft = stepHeader.AddFormattedText($"Step {num}: {Safe(step.Title)}", TextFormat.Bold);
                ft.Size = 11;

                if (!string.IsNullOrWhiteSpace(step.Details))
                {
                    var details = section.AddParagraph(step.Details);
                    details.Format.LeftIndent = "0.5cm";
                }

                if (!string.IsNullOrWhiteSpace(step.ToolsNeeded))
                {
                    var tools = section.AddParagraph();
                    tools.Format.LeftIndent = "0.5cm";
                    tools.Format.Font.Size = 9;
                    tools.AddFormattedText("Tools: ", TextFormat.Bold);
                    tools.AddText(step.ToolsNeeded);
                }

                if (!string.IsNullOrWhiteSpace(step.SafetyNotes))
                {
                    var safety = section.AddParagraph();
                    safety.Format.LeftIndent = "0.5cm";
                    safety.Format.Font.Size = 9;
                    safety.Format.Font.Color = Colors.DarkRed;
                    safety.AddFormattedText("Safety: ", TextFormat.Bold);
                    safety.AddText(step.SafetyNotes);
                }
            }
        }

        // ---- Footer --------------------------------------------------------
        var footer = section.Footers.Primary.AddParagraph();
        footer.Format.Alignment = ParagraphAlignment.Center;
        footer.Format.Font.Size = 8;
        footer.Format.Font.Color = Colors.Gray;
        footer.AddText("Evolution Maintenance  |  Generated by Quote AI Agent");
    }

    private static void AddLabelRow(Table table, string label, string value)
    {
        var row = table.AddRow();
        var l = row.Cells[0].AddParagraph(label);
        l.Format.Font.Bold = true;
        row.Cells[1].AddParagraph(value);
    }

    private static void AddTotalRow(Table table, string label, string value, bool bold)
    {
        var row = table.AddRow();
        var labelP = row.Cells[0].AddParagraph(label);
        var valueP = row.Cells[1].AddParagraph(value);
        if (bold)
        {
            labelP.Format.Font.Bold = true;
            valueP.Format.Font.Bold = true;
        }
    }

    private static string Safe(string? s) => string.IsNullOrWhiteSpace(s) ? "—" : s;

    // Renders a markup percentage. Zero collapses to an em-dash so the table
    // visually distinguishes "no markup" from "0% explicitly applied".
    private static string FormatMarkup(decimal pct) =>
        pct <= 0 ? "—" : $"{pct:0.##}%";
}
