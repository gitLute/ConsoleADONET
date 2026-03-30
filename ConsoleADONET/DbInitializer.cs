using System;
using System.Data.SqlClient;
using System.Globalization;

namespace ConsoleADONET
{
    public static class DbInitializer
    {
        public static string Initialize(string connectionString,
            int tanks_number = 75, int fuels_number = 75, int operations_number = 10000)
        {
            string result = "";

            using (SqlConnection connection = new SqlConnection(connectionString))
            {
                connection.Open();

                // ── Проверяем все три таблицы ─────────────────────────────────
                // Если хотя бы одна пуста (например, после ручной очистки),
                // очищаем все три и заполняем заново, чтобы данные были консистентны.
                SqlCommand countCmd = connection.CreateCommand();

                countCmd.CommandText = "SELECT COUNT(*) FROM Fuels;";
                int fuelsCount = (int)countCmd.ExecuteScalar();

                countCmd.CommandText = "SELECT COUNT(*) FROM Tanks;";
                int tanksCount = (int)countCmd.ExecuteScalar();

                countCmd.CommandText = "SELECT COUNT(*) FROM Operations;";
                int opsCount = (int)countCmd.ExecuteScalar();

                if (fuelsCount > 0 && tanksCount > 0 && opsCount > 0)
                    return result; // все таблицы заполнены — инициализация не нужна

                SqlTransaction transaction = connection.BeginTransaction();
                SqlCommand command = connection.CreateCommand();
                command.Transaction = transaction;

                try
                {
                    // Очищаем в правильном порядке: сначала дочерняя (FK), затем родительские
                    command.CommandText = "DELETE FROM Operations; DELETE FROM Fuels; DELETE FROM Tanks;";
                    command.ExecuteNonQuery();

                    Random randObj = new Random(1);
                    string specifier = "G";
                    CultureInfo culture = CultureInfo.InvariantCulture;
                    DateTime today = DateTime.Now.Date;

                    // ── Заполнение Tanks ──────────────────────────────────────
                    string[] tank_voc     = { "Цистерна_", "Ведро_", "Бак_", "Фляга_", "Стакан_" };
                    string[] material_voc = { "Сталь", "Платина", "Алюминий", "ПЭТ", "Чугун", "Золото", "Дерево", "Керамика" };

                    string strSql = "INSERT INTO Tanks (TankType, TankWeight, TankVolume, TankMaterial) VALUES ";
                    for (int tankId = 1; tankId <= tanks_number; tankId++)
                    {
                        string tankType     = "N'" + tank_voc[randObj.Next(tank_voc.Length)]     + tankId + "'";
                        string tankMaterial = "N'" + material_voc[randObj.Next(material_voc.Length)] + "'";
                        float  tankWeight   = 500 * (float)randObj.NextDouble();
                        float  tankVolume   = 200 * (float)randObj.NextDouble();
                        strSql += $"({tankType}, {tankWeight.ToString(specifier, culture)}, {tankVolume.ToString(specifier, culture)}, {tankMaterial}), ";
                    }
                    command.CommandText = strSql.TrimEnd(',', ' ') + ";";
                    command.ExecuteNonQuery();

                    // ── Заполнение Fuels ──────────────────────────────────────
                    string[] fuel_voc = { "Нефть_", "Бензин_", "Керосин_", "Мазут_", "Спирт_", "Водород_" };

                    strSql = "INSERT INTO Fuels (FuelType, FuelDensity) VALUES ";
                    for (int fuelId = 1; fuelId <= fuels_number; fuelId++)
                    {
                        string fuelType    = "N'" + fuel_voc[randObj.Next(fuel_voc.Length)] + fuelId + "'";
                        float  fuelDensity = 2 * (float)randObj.NextDouble();
                        strSql += $"({fuelType}, {fuelDensity.ToString(specifier, culture)}), ";
                    }
                    command.CommandText = strSql.TrimEnd(',', ' ') + ";";
                    command.ExecuteNonQuery();

                    // ── Заполнение Operations (по одной строке — FK-ключи из уже вставленных) ──
                    for (int opId = 1; opId <= operations_number; opId++)
                    {
                        int      tankId    = randObj.Next(1, tanks_number);
                        int      fuelId    = randObj.Next(1, fuels_number);
                        int      inc_exp   = randObj.Next(200) - 100;
                        DateTime opDate    = today.AddDays(-opId);
                        command.CommandText =
                            $"INSERT INTO Operations (TankId, FuelId, Inc_Exp, Date) VALUES " +
                            $"({tankId}, {fuelId}, {inc_exp.ToString(specifier, culture)}, '{opDate.ToString(culture)}');";
                        command.ExecuteNonQuery();
                    }

                    transaction.Commit();
                    Console.WriteLine($"База данных инициализирована: {tanks_number} ёмкостей, {fuels_number} видов топлива, {operations_number} операций.");
                }
                catch (Exception ex)
                {
                    result = ex.Message;
                    Console.WriteLine($"Ошибка при инициализации: {result}");
                    transaction.Rollback();
                }
            }
            return result;
        }
    }
}
