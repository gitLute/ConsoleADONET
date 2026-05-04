using System.Collections;
using System.Data;
using Microsoft.Data.SqlClient;

namespace ConsoleADONET.Data
{
    /// <summary>
    /// Двойная реализация требований п.3.2 через SqlDataAdapter (отсоединённая модель).
    /// </summary>
    public static class DataAdapterExamples
    {
        // 4. Параметрический запрос: сотрудники по должности (SqlDataAdapter)
        public static IList SelectEmployeesByPosition_DataAdapter(SqlConnection conn, string positionName)
        {
            var result = new List<string>();
            using var adapter = new SqlDataAdapter("uspGetEmployeesByPosition", conn);
            adapter.SelectCommand.CommandType = CommandType.StoredProcedure;
            adapter.SelectCommand.Parameters.AddWithValue("@PositionName", positionName);

            var table = new DataTable();
            adapter.Fill(table); // Соединение открывается/закрывается автоматически

            foreach (DataRow row in table.Rows)
                result.Add($"{row["FullName"],-20} | Должность: {row["PositionName"],-15} | Звание: {row["RankName"]}");
            return result;
        }

        // 8. Добавление водителя (SqlDataAdapter + DataTable)
        public static string InsertDriver_DataAdapter(SqlConnection conn, string fullName, string licenseNumber)
        {
            // Загружаем только схему таблицы Drivers (WHERE 1=0 не возвращает строк)
            using var adapter = new SqlDataAdapter(
                "SELECT Id, FullName, LicenseNumber, Address, LicenseExpiryDate FROM Drivers WHERE 1=0", conn);

            // SqlCommandBuilder автоматически создаст InsertCommand на основе SELECT
            using var builder = new SqlCommandBuilder(adapter);

            var table = new DataTable();
            adapter.Fill(table);

            var newRow = table.NewRow();
            newRow["FullName"] = fullName;
            newRow["LicenseNumber"] = licenseNumber;
            newRow["Address"] = "г. Минск, ул. Тестовая, 1";
            newRow["LicenseExpiryDate"] = DateTime.Today.AddYears(5);
            table.Rows.Add(newRow); // RowState = Added

            try
            {
                int rows = adapter.Update(table); // Отправляет INSERT в БД
                return $"[InsertDriver_DataAdapter] Водитель добавлен. Затронуто строк: {rows}";
            }
            catch (Exception ex)
            {
                return $"[InsertDriver_DataAdapter] Ошибка вставки: {ex.Message}";
            }
        }
    }
}