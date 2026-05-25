namespace EvoAPI.Core.Interfaces;

// Pulls usable AI input out of an uploaded file. Text-bearing formats return
// just text; images (and PDFs we couldn't extract text from) return a base64
// data URL so the caller can hand them to a vision-capable model.
public interface IFileExtractionService
{
    Task<FileExtractionResult> ExtractAsync(Stream content, string fileName, CancellationToken ct = default);
}

public class FileExtractionResult
{
    public string  Text          { get; set; } = string.Empty;
    public string? ImageDataUrl  { get; set; } // "data:image/png;base64,..." when applicable
    public string  MimeType      { get; set; } = string.Empty;
    public bool    HasUsableText => !string.IsNullOrWhiteSpace(Text);
    public bool    HasImage      => !string.IsNullOrEmpty(ImageDataUrl);
}
