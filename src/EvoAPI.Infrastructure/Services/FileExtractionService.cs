using System.Text;
using DocumentFormat.OpenXml.Packaging;
using EvoAPI.Core.Interfaces;
using Microsoft.Extensions.Logging;
using UglyToad.PdfPig;

namespace EvoAPI.Infrastructure.Services;

public class FileExtractionService : IFileExtractionService
{
    private readonly ILogger<FileExtractionService> _logger;

    public FileExtractionService(ILogger<FileExtractionService> logger)
    {
        _logger = logger;
    }

    public async Task<FileExtractionResult> ExtractAsync(Stream content, string fileName, CancellationToken ct = default)
    {
        var ext = Path.GetExtension(fileName).ToLowerInvariant();

        // Buffer to a seekable byte[] so we can both extract and (for images / PDF fallback) base64-encode.
        using var ms = new MemoryStream();
        await content.CopyToAsync(ms, ct);
        var bytes = ms.ToArray();

        switch (ext)
        {
            case ".txt":
                return new FileExtractionResult
                {
                    Text     = Encoding.UTF8.GetString(bytes),
                    MimeType = "text/plain"
                };

            case ".pdf":
                return ExtractPdf(bytes);

            case ".docx":
                return ExtractDocx(bytes);

            case ".png":
            case ".jpg":
            case ".jpeg":
                var mime = ext == ".png" ? "image/png" : "image/jpeg";
                return new FileExtractionResult
                {
                    ImageDataUrl = $"data:{mime};base64,{Convert.ToBase64String(bytes)}",
                    MimeType     = mime
                };

            default:
                throw new NotSupportedException($"File extension '{ext}' is not supported.");
        }
    }

    private FileExtractionResult ExtractPdf(byte[] bytes)
    {
        try
        {
            using var pdf = PdfDocument.Open(bytes);
            var sb = new StringBuilder();
            foreach (var page in pdf.GetPages())
            {
                sb.AppendLine(page.Text);
                sb.AppendLine();
            }

            var text = sb.ToString().Trim();

            // Scanned PDFs yield no extractable text. Fall back to handing the
            // raw bytes to the model as a vision input — gpt-4o can read PDFs
            // sent as application/pdf via the file/image content type.
            if (text.Length < 50)
            {
                _logger.LogInformation("PDF text extraction yielded only {Length} chars; falling back to vision payload.", text.Length);
                return new FileExtractionResult
                {
                    ImageDataUrl = $"data:application/pdf;base64,{Convert.ToBase64String(bytes)}",
                    MimeType     = "application/pdf"
                };
            }

            return new FileExtractionResult { Text = text, MimeType = "application/pdf" };
        }
        catch (Exception ex)
        {
            _logger.LogWarning(ex, "PdfPig failed to parse PDF; falling back to vision payload.");
            return new FileExtractionResult
            {
                ImageDataUrl = $"data:application/pdf;base64,{Convert.ToBase64String(bytes)}",
                MimeType     = "application/pdf"
            };
        }
    }

    private FileExtractionResult ExtractDocx(byte[] bytes)
    {
        using var ms = new MemoryStream(bytes);
        using var doc = WordprocessingDocument.Open(ms, false);
        var body = doc.MainDocumentPart?.Document?.Body;
        var text = body == null ? string.Empty : body.InnerText;
        return new FileExtractionResult
        {
            Text     = text,
            MimeType = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
        };
    }
}
