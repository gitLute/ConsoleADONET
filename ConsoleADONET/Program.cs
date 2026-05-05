using System.Collections;
using System.Data;
using ConsoleADONET.Data;
using Microsoft.Data.SqlClient;

namespace ConsoleADONET
{
    class Program
    {
        static void Main(string[] args)
        {
            // Чтение строки подключения из App.config
            string connectionString = System.Configuration.ConfigurationManager
                .ConnectionStrings["DefaultConnection"].ConnectionString;

            Console.WriteLine("=== Инициализация базы данных ==="); // п. 3.1
            DbInitializer.Initialize(connectionString);

            using var connection = new SqlConnection(connectionString);
            connection.Open();
            Console.WriteLine("Подключение открыто.\n");

            try
            {
                // ПУНКТ 3.2: Запросы и операции
                RunAndPrint(connection, conn => CommandExamples.SelectEmployeesWithPositions(conn),
                    "ЗАПРОС 1 | Сотрудники и их должности");
                RunAndPrint(connection, conn => CommandExamples.SelectExpiredTechInspections(conn),
                    "ЗАПРОС 2 | Автомобили с просроченным техосмотром");
                RunAndPrint(connection, conn => CommandExamples.SelectTheftsLastMonth(conn),
                    "ЗАПРОС 3 | Угоны за прошлый месяц");

                // Двойная реализация SELECT (п.3.2)
                RunAndPrint(connection, conn => CommandExamples.SelectEmployeesByPosition_Command(conn, "Должность_1"),
                    "ЗАПРОС 4 | Сотрудники по должности (SqlCommand)");
                RunAndPrint(connection, conn => DataAdapterExamples.SelectEmployeesByPosition_DataAdapter(conn, "Должность_1"),
                    "ЗАПРОС 4 | Сотрудники по должности (SqlDataAdapter)");

                RunAndPrint(connection, conn => CommandExamples.SelectCarsByDriver(conn, "Водитель 1"),
                    "ЗАПРОС 5 | Автомобили определенного владельца");
                RunAndPrint(connection, conn => CommandExamples.SelectFoundStolenCarsPivot(conn),
                    "ЗАПРОС 6 | PIVOT: Найденные угнанные авто по годам и маркам");
                RunAndPrint(connection, conn => CommandExamples.SelectTechInspectionCountByYear(conn),
                    "ЗАПРОС 7 | Количество авто, прошедших ТО, по годам");

                // Двойная реализация DML (п.3.2)
                Pause("ОПЕРАЦИЯ 8 | Добавление водителя (SqlCommand)");
                Console.WriteLine(CommandExamples.InsertDriver_Command(connection, "Иванов Иван Иванович", "AB123456"));

                Pause("ОПЕРАЦИЯ 8 | Добавление водителя (SqlDataAdapter)");
                Console.WriteLine(DataAdapterExamples.InsertDriver_DataAdapter(connection, "Петров Петр Петрович", "CD789012"));

                Pause("ОПЕРАЦИЯ 9 | Обновление данных о водителе (ID=1)");
                Console.WriteLine(CommandExamples.UpdateDriver(connection, 1, "г. Гомель, ул. Новая, 5", "2030-12-31"));

                Pause("ОПЕРАЦИЯ 10 | Удаление данных о водителе (ID=2)");
                Console.WriteLine(CommandExamples.DeleteDriver(connection, 2));

                // ПУНКТ 3.3: Атомарность и Rollback
                Pause("ДЕМОНСТРАЦИЯ 3.3 | Атомарность и откат транзакции");
                TransactionExamples.DemonstrateAtomicityAndRollback(connection);

                // ПУНКТ 3.4: Уровни изоляции транзакций
                Pause("ДЕМОНСТРАЦИЯ 3.4 | Уровни изоляции транзакций");
                RunIsolationLevelDemo(connection);
            }
            catch (SqlException ex)
            {
                Console.WriteLine($"Ошибка SQL: {ex.Message}");
            }
            finally
            {
                Console.WriteLine("\nПодключение закрыто.");
            }

            Console.WriteLine("\nГотово. Нажмите любую клавишу для выхода.");
            Console.ReadKey();
        }

        /// <summary>
        /// Интерактивное меню для запуска теста уровней изоляции в двух экземплярах.
        /// </summary>
        static void RunIsolationLevelDemo(SqlConnection connection)
        {
            Console.WriteLine("\n--- ТРЕБОВАНИЕ п.3.4: Запустите ДВА экземпляра этого приложения ---");
            Console.WriteLine("1. В ПЕРВОМ окне выберите режим '1 - Писатель'.");
            Console.WriteLine("2. Пока первый экземпляр ждет, во ВТОРОМ окне выберите '2' или '3'.");
            Console.Write("Выберите роль для этого экземпляра (1-Писатель, 2-Читатель ReadCommitted, 3-Читатель ReadUncommitted, 0-Пропуск): ");

            string choice = Console.ReadLine()?.Trim();
            switch (choice)
            {
                case "1":
                    TransactionExamples.Writer_IsolationTest(connection);
                    break;
                case "2":
                    TransactionExamples.Reader_IsolationTest(connection, IsolationLevel.ReadCommitted);
                    break;
                case "3":
                    TransactionExamples.Reader_IsolationTest(connection, IsolationLevel.ReadUncommitted);
                    break;
                default:
                    Console.WriteLine("3.4 пропущен");
                    break;
            }
        }

        static void Pause(string description)
        {
            Console.WriteLine($"\n====== {description} (нажмите любую клавишу) ======");
            Console.ReadKey();
        }

        static void RunAndPrint(SqlConnection connection, Func<SqlConnection, IList> method, string description)
        {
            Pause(description);
            IList items = method(connection);
            if (items.Count == 0)
                Console.WriteLine("(Нет данных для отображения)");
            else
                foreach (var item in items)
                    Console.WriteLine(item);
            Console.WriteLine();
        }
    }
}