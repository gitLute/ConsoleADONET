using System;
using System.Data;
using Microsoft.Data.SqlClient;

namespace ConsoleADONET.Data
{
    /// <summary>
    /// Демонстрация принципа атомарности (п. 3.3) и уровней изоляции транзакций (п. 3.4).
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
            Console.WriteLine("\n=== [ПИСАТЕЛЬ] Тест уровней изоляции (п. 3.4) ===");
            int testId = GetFirstDriverId(conn);
            string originalAddress = GetDriverField(conn, testId, "Address");

            using var transaction = conn.BeginTransaction();
            using var cmd = new SqlCommand("", conn, transaction);

            cmd.CommandText = "UPDATE Drivers SET Address = N'ГРЯЗНЫЙ_АДРЕС_ПИСАТЕЛЯ' WHERE Id = @Id";
            cmd.Parameters.AddWithValue("@Id", testId);
            cmd.ExecuteNonQuery();

            Console.WriteLine($"[ПИСАТЕЛЬ] Изменен адрес водителя ID={testId} на 'ГРЯЗНЫЙ_АДРЕС_ПИСАТЕЛЯ'.");
            Console.WriteLine("[ПИСАТЕЛЬ] Транзакция АКТИВНА. Commit НЕ выполнен. Строка заблокирована (X-lock).");
            Console.WriteLine("[ПИСАТЕЛЬ] >>> Теперь запустите ВТОРОЙ экземпляр приложения в режиме ЧИТАТЕЛЯ <<<");
            Console.WriteLine("[ПИСАТЕЛЬ] Нажмите ENTER, чтобы откатить транзакцию (Rollback) и завершить тест...");
            Console.ReadLine();

            transaction.Rollback();
            Console.WriteLine($"[ПИСАТЕЛЬ] Транзакция отменена. Адрес восстановлен на: {originalAddress}");
        }

        /// <summary>
        /// Экземпляр 2 (Читатель): пытается прочитать заблокированную строку с заданным уровнем изоляции.
        /// </summary>
        public static void Reader_IsolationTest(SqlConnection conn, IsolationLevel level)
        {
            Console.WriteLine($"\n=== [ЧИТАТЕЛЬ] Тест уровня изоляции: {level} ===");
            int testId = GetFirstDriverId(conn);

            using var transaction = conn.BeginTransaction(level);
            using var cmd = new SqlCommand("SELECT Address FROM Drivers WHERE Id = @Id", conn, transaction);
            cmd.Parameters.AddWithValue("@Id", testId);

            // Таймаут 5 сек для демонстрации: чтобы приложение не висело бесконечно, 
            // а наглядно показало блокировку и выбросило исключение.
            cmd.CommandTimeout = 5;

            try
            {
                Console.WriteLine($"[ЧИТАТЕЛЬ] Попытка чтения адреса водителя ID={testId}...");
                var result = cmd.ExecuteScalar();
                string address = result?.ToString() ?? "(NULL)";

                Console.WriteLine($"[ЧИТАТЕЛЬ] Успешно прочитано: {address}");
                if (level == IsolationLevel.ReadUncommitted)
                {
                    Console.WriteLine("[ЧИТАТЕЛЬ] ГРЯЗНОЕ ЧТЕНИЕ (Dirty Read) сработало!");
                    Console.WriteLine("[ЧИТАТЕЛЬ] Данные считаны, несмотря на то что Писатель еще не сделал Commit.");
                    Console.WriteLine("[ЧИТАТЕЛЬ] Риск: если Писатель выполнит Rollback, эти данные окажутся неверными.");
                }
                transaction.Commit();
            }
            catch (SqlException ex) when (ex.Number == -2) // -2 = Timeout Expired
            {
                Console.WriteLine($"[ЧИТАТЕЛЬ] ТАЙМАУТ! Чтение заблокировано.");
                Console.WriteLine("[ЧИТАТЕЛЬ] ОБЪЯСНЕНИЕ (ReadCommitted):");
                Console.WriteLine("   - Писатель удерживает эксклюзивную блокировку (X-lock) на строке.");
                Console.WriteLine("   - Уровень ReadCommitted разрешает читать ТОЛЬКО зафиксированные данные.");
                Console.WriteLine("   - Читатель принудительно ждет, пока Писатель освободит ресурс (Commit/Rollback).");
                Console.WriteLine("   - Это гарантирует целостность, но снижает параллелизм (приложение 'виснет').");
                transaction.Rollback();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ЧИТАТЕЛЬ] Ошибка: {ex.Message}");
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