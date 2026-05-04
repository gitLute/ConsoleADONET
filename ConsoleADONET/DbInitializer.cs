using System;
using System.Collections.Generic;
using Microsoft.Data.SqlClient;

namespace ConsoleADONET
{
    public static class DbInitializer
    {
        // Количество записей по требованию п.3.1
        private const int DictCount = 100;      // Сторона "один"
        private const int OperCount = 10000;    // Сторона "многие"

        public static void Initialize(string connectionString)
        {
            using var conn = new SqlConnection(connectionString);
            conn.Open();

            // Проверка: если хотя бы одна таблица уже заполнена, пропускаем инициализацию
            using var checkCmd = new SqlCommand("SELECT COUNT(*) FROM Positions;", conn);
            if ((int)checkCmd.ExecuteScalar() > 0)
            {
                Console.WriteLine("База данных уже инициализирована. Пропуск генерации данных.");
                return;
            }

            using var tx = conn.BeginTransaction();
            try
            {
                // Очистка в обратном порядке зависимостей
                new SqlCommand("DELETE FROM StolenCars; DELETE FROM Cars; DELETE FROM Employees; DELETE FROM Drivers; DELETE FROM CarBrands; DELETE FROM Ranks; DELETE FROM Positions;", conn, tx).ExecuteNonQuery();

                var rnd = new Random(42);
                var posIds = new List<int>();
                var rankIds = new List<int>();
                var brandIds = new List<int>();
                var driverIds = new List<int>();
                var empIds = new List<int>();
                var carIds = new List<int>();

                Console.WriteLine("Генерация справочников (≥100 записей)...");
                
                // 1. Positions (100)
                for (int i = 1; i <= DictCount; i++)
                    posIds.Add(ExecuteInsertSp(conn, tx, "uspInsertPosition",
                        ("@Name", $"Должность_{i}"), ("@Salary", 1000 + rnd.Next(2000)),
                        ("@Responsibilities", "Контроль"), ("@Requirements", "Образование")));

                // 2. Ranks (100)
                for (int i = 1; i <= DictCount; i++)
                    rankIds.Add(ExecuteInsertSp(conn, tx, "uspInsertRank",
                        ("@Name", $"Звание_{i}"), ("@Allowance", 500 + rnd.Next(1000)),
                        ("@Responsibilities", "Руководство"), ("@Requirements", "Стаж")));

                // 3. CarBrands (100)
                string[] countries = { "Беларусь", "Германия", "Япония", "США", "Китай" };
                for (int i = 1; i <= DictCount; i++)
                    brandIds.Add(ExecuteInsertSp(conn, tx, "uspInsertCarBrand",
                        ("@Name", $"Марка_{i}"), ("@Manufacturer", $"Завод_{i}"),
                        ("@CountryOfOrigin", countries[rnd.Next(countries.Length)]),
                        ("@Category", "B"), ("@Description", "Легковой")));

                // 4. Drivers (100)
                for (int i = 1; i <= DictCount; i++)
                    driverIds.Add(ExecuteInsertSp(conn, tx, "uspInsertDriver",
                        ("@FullName", $"Водитель {i}"), ("@BirthDate", DateTime.Today.AddYears(-rnd.Next(20, 50))),
                        ("@LicenseNumber", $"LIC{i:D6}"), ("@LicenseCategory", "B")));

                Console.WriteLine("Генерация оперативных таблиц (≥10000 записей)...");

                // 5. Employees (100) -> зависит от Positions и Ranks
                for (int i = 1; i <= DictCount; i++)
                    empIds.Add(ExecuteInsertSp(conn, tx, "uspInsertEmployee",
                        ("@FullName", $"Сотрудник {i}"), ("@Age", 25 + rnd.Next(30)),
                        ("@Gender", rnd.Next(2) == 0 ? "М" : "Ж"),
                        ("@PositionId", posIds[rnd.Next(posIds.Count)]),
                        ("@RankId", rankIds[rnd.Next(rankIds.Count)])));

                // 6. Cars (10000) -> зависит от Drivers, CarBrands, Employees
                string[] colors = { "Черный", "Белый", "Синий", "Красный", "Серый" };
                for (int i = 1; i <= OperCount; i++)
                {
                    int carId = ExecuteInsertSp(conn, tx, "uspInsertCar",
                        ("@DriverId", driverIds[rnd.Next(driverIds.Count)]),
                        ("@BrandId", brandIds[rnd.Next(brandIds.Count)]),
                        ("@RegistrationNumber", $"{rnd.Next(1000, 9999)} AB-{rnd.Next(1, 8)}"),
                        ("@Color", colors[rnd.Next(colors.Length)]),
                        ("@TechInspectionStatus", "Пройден"),
                        ("@RegisteringEmployeeId", empIds[rnd.Next(empIds.Count)]));
                    carIds.Add(carId);
                }

                // 7. StolenCars (10000) -> зависит от Cars, Employees
                string[] circumstances = { "Угнан с парковки", "Взлом замка", "Разбойное нападение", "Эвакуация" };
                for (int i = 1; i <= OperCount; i++)
                {
                    DateTime theftDate = DateTime.Today.AddDays(-rnd.Next(1, 365));
                    ExecuteInsertSp(conn, tx, "uspInsertStolenCar",
                        ("@TheftDate", theftDate),
                        ("@ReportDate", theftDate.AddHours(rnd.Next(1, 12))),
                        ("@CarId", carIds[rnd.Next(carIds.Count)]),
                        ("@TheftCircumstances", circumstances[rnd.Next(circumstances.Length)]),
                        ("@IsFound", rnd.Next(2) == 1),
                        ("@RegisteringEmployeeId", empIds[rnd.Next(empIds.Count)]));
                }

                tx.Commit();
                Console.WriteLine($"Инициализация завершена: {DictCount} справочных записей, {OperCount * 2} оперативных записей.");
            }
            catch (Exception ex)
            {
                tx.Rollback();
                Console.WriteLine($"Ошибка инициализации БД: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Универсальный вызов хранимой процедуры вставки с возвратом @NewId
        /// </summary>
        private static int ExecuteInsertSp(SqlConnection conn, SqlTransaction tx, string spName, params (string Name, object Value)[] parameters)
        {
            using var cmd = new SqlCommand(spName, conn, tx) { CommandType = System.Data.CommandType.StoredProcedure };
            
            foreach (var p in parameters)
                cmd.Parameters.AddWithValue(p.Name, p.Value ?? DBNull.Value);

            var outParam = new SqlParameter("@NewId", System.Data.SqlDbType.Int) { Direction = System.Data.ParameterDirection.Output };
            cmd.Parameters.Add(outParam);

            cmd.ExecuteNonQuery();
            return (int)outParam.Value;
        }
    }
}