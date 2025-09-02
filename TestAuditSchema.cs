using System;
using System.Data.SqlClient;
using System.Threading.Tasks;

class Program
{
    static async Task Main(string[] args)
    {
        var connectionString = "Server=tcp:evotest.database.windows.net,1433;Initial Catalog=EVO;User ID=EvoUser;Password=4gKpQ4!t?H#kypq*;MultipleActiveResultSets=False;Encrypt=True;TrustServerCertificate=False;Connection Timeout=30;";
        
        try
        {
            using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync();
            
            Console.WriteLine("Connected to database successfully!");
            
            // Query to get column information for the Audit table
            var query = @"
                SELECT 
                    COLUMN_NAME, 
                    DATA_TYPE, 
                    IS_NULLABLE,
                    CHARACTER_MAXIMUM_LENGTH
                FROM INFORMATION_SCHEMA.COLUMNS 
                WHERE TABLE_NAME = 'Audit' 
                ORDER BY ORDINAL_POSITION";
            
            using var command = new SqlCommand(query, connection);
            using var reader = await command.ExecuteReaderAsync();
            
            Console.WriteLine("\nAudit table columns:");
            Console.WriteLine("Column Name\t\tData Type\tNullable\tMax Length");
            Console.WriteLine("==========================================================");
            
            while (await reader.ReadAsync())
            {
                var columnName = reader["COLUMN_NAME"].ToString();
                var dataType = reader["DATA_TYPE"].ToString();
                var isNullable = reader["IS_NULLABLE"].ToString();
                var maxLength = reader["CHARACTER_MAXIMUM_LENGTH"].ToString();
                
                Console.WriteLine($"{columnName,-20}\t{dataType,-12}\t{isNullable,-8}\t{maxLength}");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error: {ex.Message}");
        }
    }
}
