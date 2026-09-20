using System.Data;
using EvoAPI.Shared.DTOs;

namespace EvoAPI.Api.Services;

/// Turns the raw High Volume / Metro Pipe result sets into DTOs. Shared by the admin
/// ReportsController and the anonymous PublicReportsController so both serve identical shapes.
public static class MetroPipeReportMapper
{
    public static List<HighVolumeReportDto> ToHighVolumeReport(DataTable dataTable)
    {
        var result = new List<HighVolumeReportDto>();

        foreach (DataRow row in dataTable.Rows)
        {
            result.Add(new HighVolumeReportDto
            {
                Tech = CleanString(row["Tech"]),
                Today = ConvertToInt(row["Today"]),
                Previous1 = ConvertToInt(row["Previous_1"]),
                Previous2 = ConvertToInt(row["Previous_2"]),
                Previous3 = ConvertToInt(row["Previous_3"]),
                Previous4 = ConvertToInt(row["Previous_4"]),
                TodayName = CleanString(row["Today_Name"]),
                Previous1Name = CleanString(row["Previous_1_Name"]),
                Previous2Name = CleanString(row["Previous_2_Name"]),
                Previous3Name = CleanString(row["Previous_3_Name"]),
                Previous4Name = CleanString(row["Previous_4_Name"]),
                NotCompleted = ConvertToInt(row["NotCompleted"])
            });
        }

        return result;
    }

    /// Result-set order is fixed by DataService.GetMetroPipeSummaryAsync:
    /// 0 XRF tech rows, 1 XRF crews, 2 HV crews, 3 waves, 4 results, 5 scalars.
    public static MetroPipeReportDto ToMetroPipeReport(DataSet dataSet)
    {
        var report = new MetroPipeReportDto();
        if (dataSet.Tables.Count < 6)
            throw new InvalidOperationException($"Metro Pipe summary returned {dataSet.Tables.Count} result sets, expected 6");

        foreach (DataRow row in dataSet.Tables[0].Rows)
        {
            report.XrfRows.Add(new XrfReportRowDto
            {
                Tech = CleanString(row["Tech"]),
                Today = ConvertToInt(row["Today"]),
                Previous1 = ConvertToInt(row["Previous_1"]),
                Previous2 = ConvertToInt(row["Previous_2"]),
                Previous3 = ConvertToInt(row["Previous_3"]),
                Previous4 = ConvertToInt(row["Previous_4"]),
                Complete = ConvertToInt(row["Complete"]),
                TodayName = CleanString(row["Today_Name"]),
                Previous1Name = CleanString(row["Previous_1_Name"]),
                Previous2Name = CleanString(row["Previous_2_Name"]),
                Previous3Name = CleanString(row["Previous_3_Name"]),
                Previous4Name = CleanString(row["Previous_4_Name"])
            });
        }

        static CrewReportRowDto Crew(DataRow row) => new()
        {
            Crew = CleanString(row["Crew"]),
            Today = ConvertToInt(row["Today"]),
            Previous1 = ConvertToInt(row["Previous_1"]),
            Previous2 = ConvertToInt(row["Previous_2"]),
            Previous3 = ConvertToInt(row["Previous_3"]),
            Previous4 = ConvertToInt(row["Previous_4"])
        };
        foreach (DataRow row in dataSet.Tables[1].Rows) report.XrfCrews.Add(Crew(row));
        foreach (DataRow row in dataSet.Tables[2].Rows) report.HvCrews.Add(Crew(row));

        foreach (DataRow row in dataSet.Tables[3].Rows)
        {
            var total = ConvertToInt(row["Total"]);
            var done = ConvertToInt(row["Done"]);
            report.XrfWaves.Add(new XrfWaveProgressDto
            {
                BatchId = ConvertToInt(row["xrfb_id"]),
                Wave = CleanString(row["xrfb_filename"]),
                LoadedDate = row["xrfb_insertdatetime"] is DateTime dt ? dt : DateTime.MinValue,
                Total = total,
                Done = done,
                Pending = Math.Max(0, total - done)
            });
        }

        foreach (DataRow row in dataSet.Tables[4].Rows)
        {
            report.XrfResults.Add(new XrfResultCountDto
            {
                Result = CleanString(row["Result"]),
                Count = ConvertToInt(row["Count"])
            });
        }

        if (dataSet.Tables[5].Rows.Count > 0)
        {
            var row = dataSet.Tables[5].Rows[0];
            report.XrfPending = ConvertToInt(row["XrfPending"]);
            report.XrfWaveName = row["XrfWaveName"] == DBNull.Value ? null : CleanString(row["XrfWaveName"]);
            report.XrfWaveTotal = ConvertToInt(row["XrfWaveTotal"]);
            report.XrfDone = ConvertToInt(row["XrfDone"]);
            report.HvWaveName = row["HvWaveName"] == DBNull.Value ? null : CleanString(row["HvWaveName"]);
            report.HvWaveTotal = ConvertToInt(row["HvWaveTotal"]);
            report.HvDone = ConvertToInt(row["HvDone"]);
        }

        return report;
    }

    private static string CleanString(object value) =>
        value == null || value == DBNull.Value ? string.Empty : value.ToString()?.Trim() ?? string.Empty;

    private static int ConvertToInt(object value)
    {
        if (value == null || value == DBNull.Value) return 0;
        return int.TryParse(value.ToString(), out var result) ? result : 0;
    }
}
