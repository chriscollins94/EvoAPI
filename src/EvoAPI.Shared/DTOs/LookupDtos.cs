namespace EvoAPI.Shared.DTOs
{
    // Lookup-admin shapes for the Settings → Lookup Tables pages (camelCase via JSON).
    // Named with a *LookupDto suffix where a differently-shaped DTO of the base name
    // already exists elsewhere in the codebase.

    public class ContactTitleLookupDto
    {
        public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
    }

    public class AddressTitleLookupDto
    {
        public int Id { get; set; }
        public string Title { get; set; } = string.Empty;
    }

    public class PaymentMethodDto
    {
        public int Id { get; set; }
        public string Name { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public int Order { get; set; }
        public bool Active { get; set; }
    }

    public class ServiceItemUnitLookupDto
    {
        public int Id { get; set; }
        public string Unit { get; set; } = string.Empty;
        public string Description { get; set; } = string.Empty;
        public int Order { get; set; }
    }

    public class TermsLookupDto
    {
        public int Id { get; set; }
        public string Description { get; set; } = string.Empty;
        public int NumberOfDays { get; set; }
        public int Order { get; set; }
    }
}
