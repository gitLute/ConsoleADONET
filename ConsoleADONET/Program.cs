using System.Collections;
using ConsoleADONET.Data;
using Microsoft.Data.SqlClient;

namespace ConsoleADONET
{
    class Program
    {
        static void Main(string[] args)
        {
            string connectionString = System.Configuration.ConfigurationManager.ConnectionStrings["DefaultConnection"].ConnectionString;

            Console.WriteLine("=== Инициализация базы данных (п. 3.1) ===");
            DbInitializer.Initialize(connectionString);

            using var connection = new SqlConnection(connectionString);
            connection.Open();
            Console.WriteLine("Подключение открыто.\n");

            try
            {
                RunAndPrint(connection, conn => CommandExamples.SelectEmployeesWithPositions(conn),
                    "ЗАПРОС 1 | Сотрудники и их должности");

                RunAndPrint(connection, conn => CommandExamples.SelectExpiredTechInspections(conn),
                    "ЗАПРОС 2 | Автомобили с просроченным техосмотром");

                RunAndPrint(connection, conn => CommandExamples.SelectTheftsLastMonth(conn),
                    "ЗАПРОС 3 | Угоны за прошлый месяц");

                // Двойная реализация SELECT (требование п.3.2)
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

                // Двойная реализация DML (требование п.3.2)
                Pause("ОПЕРАЦИЯ 8 | Добавление водителя (SqlCommand)");
                Console.WriteLine(CommandExamples.InsertDriver_Command(connection, "Иванов Иван Иванович", "AB123456"));

                Pause("ОПЕРАЦИЯ 8 | Добавление водителя (SqlDataAdapter)");
                Console.WriteLine(DataAdapterExamples.InsertDriver_DataAdapter(connection, "Петров Петр Петрович", "CD789012"));

                Pause("ОПЕРАЦИЯ 9 | Обновление данных о водителе (ID=1)");
                Console.WriteLine(CommandExamples.UpdateDriver(connection, 1, "г. Гомель, ул. Новая, 5", "2030-12-31"));

                Pause("ОПЕРАЦИЯ 10 | Удаление данных о водителе (ID=2)");
                Console.WriteLine(CommandExamples.DeleteDriver(connection, 2));
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