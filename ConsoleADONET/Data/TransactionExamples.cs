using System;
using System.Data;
using System.Threading;
using Microsoft.Data.SqlClient;

namespace ConsoleADONET.Data
{
    /// <summary>
    /// Демонстрация принципа атомарности (п. 3.3), уровней изоляции (п. 3.4) и взаимных блокировок (п. 3.5).
    /// </summary>
    public static class TransactionExamples
    {
        public static void DemonstrateAtomicityAndRollback(SqlConnection conn)
        {
            Console.WriteLine("--- Старт теста атомарности (п. 3.3) ---");
            int testId = GetFirstDriverId(conn);
            string initialAddress = GetDriverField(conn, testId, "Address");
            Console.WriteLine($"[ДО ТРАНЗАКЦИИ] Адрес водителя ID={testId}: {initialAddress}");

            SqlTransaction transaction = null;
            try
            {
                transaction = conn.BeginTransaction();
                using var cmd = new SqlCommand("", conn, transaction);

                // Шаг 1: Успешное обновление
                cmd.CommandText = "UPDATE Drivers SET Address = N'ВРЕМЕННЫЙ_АДРЕС_ТРАНЗАКЦИИ' WHERE Id = @Id";
                cmd.Parameters.AddWithValue("@Id", testId);
                int rows1 = cmd.ExecuteNonQuery();
                Console.WriteLine($"[ШАГ 1] Выполнен UPDATE. Затронуто строк: {rows1}. Данные изменены в памяти транзакции.");

                // Шаг 2: Имитация сбоя
                Console.WriteLine("[ШАГ 2] Попытка выполнения второй части пакета...");
                throw new InvalidOperationException("Искусственный сбой! Вторая операция пакета не выполнена.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ИСКЛЮЧЕНИЕ] {ex.Message}");
                if (transaction != null)
                {
                    transaction.Rollback();
                    Console.WriteLine("[ROLLBACK] Транзакция отменена. Все изменения текущего пакета аннулированы.");
                }
            }
            finally
            {
                transaction?.Dispose();
            }

            string finalAddress = GetDriverField(conn, testId, "Address");
            Console.WriteLine($"[ПОСЛЕ ОТКАТА] Адрес водителя ID={testId}: {finalAddress}");
            Console.WriteLine(initialAddress == finalAddress
                ? "УСПЕХ: Данные вернулись к исходному состоянию. Принцип атомарности подтвержден."
                : "ВНИМАНИЕ: Данные изменились. Откат не сработал корректно.");
            Console.WriteLine();
        }

        /// <summary>
        /// Экземпляр 1 (Писатель): изменяет строку, но не фиксирует транзакцию.
        /// </summary>
        public static void Writer_IsolationTest(SqlConnection conn)
        {
            Console.WriteLine("\n=== [Writer_IsolationTest] Тест уровней изоляции (п. 3.4) ===");
            int testId = GetFirstDriverId(conn);
            string originalAddress = GetDriverField(conn, testId, "Address");

            using var transaction = conn.BeginTransaction();
            using var cmd = new SqlCommand("", conn, transaction);

            cmd.CommandText = "UPDATE Drivers SET Address = N'ГРЯЗНЫЙ_АДРЕС_ПИСАТЕЛЯ' WHERE Id = @Id";
            cmd.Parameters.AddWithValue("@Id", testId);
            cmd.ExecuteNonQuery();

            Console.WriteLine($"[Writer_IsolationTest] Изменен адрес водителя ID={testId} на 'ГРЯЗНЫЙ_АДРЕС_ПИСАТЕЛЯ'.");
            // Console.WriteLine("[ПИСАТЕЛЬ] Транзакция АКТИВНА. Commit НЕ выполнен. Строка заблокирована (X-lock).");
            // Console.WriteLine("[ПИСАТЕЛЬ] >>> Теперь запустите ВТОРОЙ экземпляр приложения в режиме ЧИТАТЕЛЯ <<<");
            Console.WriteLine("[Writer_IsolationTest] Нажмите ENTER, чтобы откатить транзакцию (Rollback) и завершить тест...");
            Console.ReadLine();

            transaction.Rollback();
            Console.WriteLine($"[Writer_IsolationTest] Транзакция отменена. Адрес восстановлен на: {originalAddress}");
        }

        /// <summary>
        /// Экземпляр 2 (Читатель): пытается прочитать заблокированную строку с заданным уровнем изоляции.
        /// </summary>
        public static void Reader_IsolationTest(SqlConnection conn, IsolationLevel level)
        {
            Console.WriteLine($"\n=== [Reader_IsolationTest] Тест уровня изоляции: {level} ===");
            int testId = GetFirstDriverId(conn);

            using var transaction = conn.BeginTransaction(level);
            using var cmd = new SqlCommand("SELECT Address FROM Drivers WHERE Id = @Id", conn, transaction);
            cmd.Parameters.AddWithValue("@Id", testId);

            // Таймаут 5 сек для демонстрации блокировки
            cmd.CommandTimeout = 5;

            try
            {
                Console.WriteLine($"[Reader_IsolationTest] Попытка чтения адреса водителя ID={testId}...");
                var result = cmd.ExecuteScalar();
                string address = result?.ToString() ?? "(NULL)";

                Console.WriteLine($"[Reader_IsolationTest] Успешно прочитано: {address}");
                if (level == IsolationLevel.ReadUncommitted)
                {
                    Console.WriteLine("[Reader_IsolationTest] ГРЯЗНОЕ ЧТЕНИЕ (Dirty Read) сработало!");
                }
                transaction.Commit();
            }
            catch (SqlException ex) when (ex.Number == -2) // -2 = Timeout Expired
            {
                Console.WriteLine($"[Reader_IsolationTest] ТАЙМАУТ! Чтение заблокировано.");
                transaction.Rollback();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[Reader_IsolationTest] Ошибка: {ex.Message}");
                transaction.Rollback();
            }
        }

        /// <summary>
        /// Поток А: Блокирует Ресурс 1 (ID=1), ждет, затем пытается заблокировать Ресурс 2 (ID=2).
        /// </summary>
        public static void Deadlock_ThreadA(SqlConnection conn)
        {
            Console.WriteLine("\n=== [ПОТОК А] Тест Deadlock (п. 3.5) ===");
            int id1 = 1;
            int id2 = 2;

            using var transaction = conn.BeginTransaction();
            using var cmd = new SqlCommand("", conn, transaction);

            try
            {
                // 1. Захват Ресурса 1
                cmd.CommandText = "UPDATE Drivers SET Address = N'LOCKED_BY_A_1' WHERE Id = @Id";
                cmd.Parameters.AddWithValue("@Id", id1);
                cmd.ExecuteNonQuery();
                Console.WriteLine($"[ПОТОК А] Захвачен Ресурс 1 (Driver ID={id1}). Жду 3 сек...");

                Thread.Sleep(3000); // Имитация работы, даем время Потоку Б захватить Ресурс 2

                // 2. Попытка захвата Ресурса 2
                Console.WriteLine($"[ПОТОК А] Пытаюсь захватить Ресурс 2 (Driver ID={id2})...");
                cmd.Parameters.Clear();
                cmd.CommandText = "UPDATE Drivers SET Address = N'LOCKED_BY_A_2' WHERE Id = @Id";
                cmd.Parameters.AddWithValue("@Id", id2);
                cmd.ExecuteNonQuery(); // Здесь возникнет ожидание или Deadlock

                transaction.Commit();
                Console.WriteLine("[ПОТОК А] Успешно завершен. Deadlock не произошел.");
            }
            catch (SqlException ex) when (ex.Number == 1205)
            {
                Console.WriteLine($"[ПОТОК А] Ошибка SQL {ex.Number}: {ex.Message}");
                transaction.Rollback();
                Console.WriteLine("[ПОТОК А] Транзакция отменена. Блокировки сняты. Второй поток продолжит работу.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ПОТОК А] ❌ Ошибка: {ex.Message}");
                transaction.Rollback();
            }
        }

        /// <summary>
        /// Поток Б: Блокирует Ресурс 2 (ID=2), ждет, затем пытается заблокировать Ресурс 1 (ID=1).
        /// </summary>
        public static void Deadlock_ThreadB(SqlConnection conn)
        {
            Console.WriteLine("\n=== [ПОТОК Б] Тест Deadlock (п. 3.5) ===");
            int id1 = 1;
            int id2 = 2;

            using var transaction = conn.BeginTransaction();
            using var cmd = new SqlCommand("", conn, transaction);

            try
            {
                // 1. Захват Ресурса 2 (ОБРАТНЫЙ ПОРЯДОК!)
                cmd.CommandText = "UPDATE Drivers SET Address = N'LOCKED_BY_B_2' WHERE Id = @Id";
                cmd.Parameters.AddWithValue("@Id", id2);
                cmd.ExecuteNonQuery();
                Console.WriteLine($"[ПОТОК Б] Захвачен Ресурс 2 (Driver ID={id2}). Жду 3 сек...");

                Thread.Sleep(3000); // Имитация работы, даем время Потоку А захватить Ресурс 1

                // 2. Попытка захвата Ресурса 1
                Console.WriteLine($"[ПОТОК Б] Пытаюсь захватить Ресурс 1 (Driver ID={id1})...");
                cmd.Parameters.Clear();
                cmd.CommandText = "UPDATE Drivers SET Address = N'LOCKED_BY_B_1' WHERE Id = @Id";
                cmd.Parameters.AddWithValue("@Id", id1);
                cmd.ExecuteNonQuery(); // Здесь возникнет ожидание или Deadlock

                transaction.Commit();
                Console.WriteLine("[ПОТОК Б] Успешно завершен. Deadlock не произошел.");
            }
            catch (SqlException ex) when (ex.Number == 1205)
            {
                Console.WriteLine($"[ПОТОК Б] Ошибка SQL {ex.Number}: {ex.Message}");
                transaction.Rollback();
                Console.WriteLine("[ПОТОК Б] Транзакция отменена. Блокировки сняты. Первый поток продолжит работу.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ПОТОК Б] ❌ Ошибка: {ex.Message}");
                transaction.Rollback();
            }
        }

        private static int GetFirstDriverId(SqlConnection conn)
        {
            using var cmd = new SqlCommand("SELECT TOP 1 Id FROM Drivers ORDER BY Id", conn);
            var res = cmd.ExecuteScalar();
            if (res == null || res == DBNull.Value)
                throw new InvalidOperationException("Таблица Drivers пуста. Сначала выполните инициализацию БД.");
            return Convert.ToInt32(res);
        }

        private static string GetDriverField(SqlConnection conn, int id, string fieldName)
        {
            string query = $"SELECT {fieldName} FROM Drivers WHERE Id = @Id";
            using var cmd = new SqlCommand(query, conn);
            cmd.Parameters.AddWithValue("@Id", id);
            var result = cmd.ExecuteScalar();
            return result == null || result == DBNull.Value ? "(NULL)" : result.ToString();
        }
    }
}