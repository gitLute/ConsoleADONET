using System.Collections;
using System.Data;
using Microsoft.Data.SqlClient;

namespace ConsoleADONET.Data
{
    /// <summary>
    /// Реализация запросов и операций Варианта 21 (ГАИ) через SqlCommand.
    /// </summary>
    public static class CommandExamples
    {
        // 1. Данные о сотрудниках и должностях
        public static IList SelectEmployeesWithPositions(SqlConnection conn)
        {
            var result = new List<string>();
            using var cmd = new SqlCommand("uspGetEmployeesByPosition", conn) { CommandType = CommandType.StoredProcedure };
            cmd.Parameters.AddWithValue("@PositionName", ""); // Пустой параметр = выборка всех
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                result.Add($"{reader["FullName"],-20} | {reader["PositionName"],-15} | {reader["RankName"]}");
            return result;
        }

        // 2. Список автомобилей с просроченным техосмотром
        public static IList SelectExpiredTechInspections(SqlConnection conn)
        {
            var result = new List<string>();
            using var cmd = new SqlCommand("uspGetExpiredTechInspections", conn) { CommandType = CommandType.StoredProcedure };
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                result.Add($"Авто: {reader["RegistrationNumber"],-10} | Владелец: {reader["DriverName"],-20} | Дата ТО: {reader["TechInspectionDate"]:d}");
            return result;
        }

        // 3. Список угонов за прошлый месяц
        public static IList SelectTheftsLastMonth(SqlConnection conn)
        {
            var result = new List<string>();
            using var cmd = new SqlCommand("uspGetTheftsLastMonth", conn) { CommandType = CommandType.StoredProcedure };
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                result.Add($"Угон #{reader["Id"],-4} | Авто: {reader["RegistrationNumber"],-10} | Дата: {reader["TheftDate"]:d} | Найдено: {reader["IsFound"]}");
            return result;
        }

        // 4. Параметрический запрос: сотрудники по должности (SqlCommand)
        public static IList SelectEmployeesByPosition_Command(SqlConnection conn, string positionName)
        {
            var result = new List<string>();
            using var cmd = new SqlCommand("uspGetEmployeesByPosition", conn) { CommandType = CommandType.StoredProcedure };
            cmd.Parameters.AddWithValue("@PositionName", positionName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                result.Add($"{reader["FullName"],-20} | Должность: {reader["PositionName"],-15} | Звание: {reader["RankName"]}");
            return result;
        }

        // 5. Параметрический запрос: авто по владельцу
        public static IList SelectCarsByDriver(SqlConnection conn, string driverName)
        {
            var result = new List<string>();
            using var cmd = new SqlCommand("uspGetCarsByDriver", conn) { CommandType = CommandType.StoredProcedure };
            cmd.Parameters.AddWithValue("@DriverName", driverName);
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                result.Add($"Авто: {reader["RegistrationNumber"],-10} | Марка: {reader["BrandName"],-15} | Цвет: {reader["Color"]}");
            return result;
        }

        // 6. Перекрестный запрос (PIVOT): найденные угнанные авто по годам и маркам
        public static IList SelectFoundStolenCarsPivot(SqlConnection conn)
        {
            var result = new List<string>();
            using var cmd = new SqlCommand("uspGetFoundStolenCarsPivot", conn) { CommandType = CommandType.StoredProcedure };
            using var reader = cmd.ExecuteReader();
            result.Add($"{"Марка",-15} | {"2022",-6} | {"2023",-6} | {"2024",-6} | {"2025",-6}");
            while (reader.Read())
                result.Add($"{reader["BrandName"],-15} | {reader["2022"],-6} | {reader["2023"],-6} | {reader["2024"],-6} | {reader["2025"],-6}");
            return result;
        }

        // 7. Количество авто, прошедших ТО, по годам
        public static IList SelectTechInspectionCountByYear(SqlConnection conn)
        {
            var result = new List<string>();
            using var cmd = new SqlCommand("uspGetTechInspectionCountByYear", conn) { CommandType = CommandType.StoredProcedure };
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
                result.Add($"Год: {reader["InspectionYear"]} | Пройдено ТО: {reader["CarsCount"]} авто");
            return result;
        }

        // 8. Добавление водителя (SqlCommand)
        public static string InsertDriver_Command(SqlConnection conn, string fullName, string licenseNumber)
        {
            using var cmd = new SqlCommand("uspInsertDriver", conn) { CommandType = CommandType.StoredProcedure };
            cmd.Parameters.AddWithValue("@FullName", fullName);
            cmd.Parameters.AddWithValue("@LicenseNumber", licenseNumber);
            cmd.Parameters.Add("@NewId", SqlDbType.Int).Direction = ParameterDirection.Output;

            using var tx = conn.BeginTransaction();
            cmd.Transaction = tx;
            try
            {
                cmd.ExecuteNonQuery();
                tx.Commit();
                return $"[InsertDriver_Command] Водитель добавлен. ID = {cmd.Parameters["@NewId"].Value}";
            }
            catch (Exception ex)
            {
                tx.Rollback();
                return $"[InsertDriver_Command] Ошибка вставки: {ex.Message}. Транзакция отменена.";
            }
        }

        // 9. Обновление данных о водителе
        public static string UpdateDriver(SqlConnection conn, int id, string newAddress, string newLicenseExpiry)
        {
            using var cmd = new SqlCommand("uspUpdateDriver", conn) { CommandType = CommandType.StoredProcedure };
            cmd.Parameters.AddWithValue("@Id", id);
            cmd.Parameters.AddWithValue("@Address", newAddress);
            cmd.Parameters.AddWithValue("@LicenseExpiryDate", DateTime.Parse(newLicenseExpiry));

            using var tx = conn.BeginTransaction();
            cmd.Transaction = tx;
            try
            {
                int rows = cmd.ExecuteNonQuery();
                tx.Commit();
                return $"[UpdateDriver] Водитель ID={id} обновлен. Затронуто строк: {rows}";
            }
            catch (Exception ex)
            {
                tx.Rollback();
                return $"[UpdateDriver] Ошибка обновления: {ex.Message}";
            }
        }

        // 10. Удаление данных о водителе
        public static string DeleteDriver(SqlConnection conn, int id)
        {
            using var cmd = new SqlCommand("uspDeleteDriver", conn) { CommandType = CommandType.StoredProcedure };
            cmd.Parameters.AddWithValue("@Id", id);

            using var tx = conn.BeginTransaction();
            cmd.Transaction = tx;
            try
            {
                int rows = cmd.ExecuteNonQuery();
                tx.Commit();
                return $"[DeleteDriver] Водитель ID={id} удален. Затронуто строк: {rows}";
            }
            catch (Exception ex)
            {
                tx.Rollback();
                return $"[DeleteDriver] Ошибка удаления: {ex.Message}";
            }
        }
    }
}