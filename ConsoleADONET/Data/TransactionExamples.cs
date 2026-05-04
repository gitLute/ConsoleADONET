using System;
using System.Data;
using Microsoft.Data.SqlClient;

namespace ConsoleADONET.Data
{
    /// <summary>
    /// Демонстрация принципа атомарности (Atomicity) и отката транзакции (Rollback) по п. 3.3.
    /// </summary>
    public static class TransactionExamples
    {
        public static void DemonstrateAtomicityAndRollback(SqlConnection conn)
        {
            Console.WriteLine("--- Старт теста атомарности (п. 3.3) ---");

            // 1. Фиксируем исходное состояние данных до транзакции
            string initialAddress = GetDriverField(conn, 1, "Address");
            Console.WriteLine($"[ДО ТРАНЗАКЦИИ] Адрес водителя ID=1: {initialAddress}");

            SqlTransaction transaction = null;
            try
            {
                // Начало явной транзакции
                transaction = conn.BeginTransaction();
                using var cmd = new SqlCommand("", conn, transaction);

                // Шаг 1: Первая часть пакетного изменения (выполняется успешно)
                cmd.CommandText = "UPDATE Drivers SET Address = N'ВРЕМЕННЫЙ_АДРЕС_ТРАНЗАКЦИИ' WHERE Id = 1";
                int rows1 = cmd.ExecuteNonQuery();
                Console.WriteLine($"[ШАГ 1] Выполнен UPDATE. Затронуто строк: {rows1}. Данные изменены в памяти транзакции.");

                // Шаг 2: Имитация критического сбоя перед завершением пакета
                Console.WriteLine("[ШАГ 2] Попытка выполнения второй части пакета...");
                throw new InvalidOperationException("Искусственный сбой! Вторая операция пакета не выполнена.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[ИСКЛЮЧЕНИЕ] {ex.Message}");
                if (transaction != null)
                {
                    // Откат всех изменений, сделанных в рамках данной транзакции
                    transaction.Rollback();
                    Console.WriteLine("[ROLLBACK] Транзакция отменена. Все изменения текущего пакета аннулированы.");
                }
            }
            finally
            {
                transaction?.Dispose();
            }

            // 3. Проверка состояния БД после отката
            string finalAddress = GetDriverField(conn, 1, "Address");
            Console.WriteLine($"[ПОСЛЕ ОТКАТА] Адрес водителя ID=1: {finalAddress}");

            if (initialAddress == finalAddress)
                Console.WriteLine("УСПЕХ: Данные вернулись к исходному состоянию. Принцип атомарности (Atomicity) подтвержден.");
            else
                Console.WriteLine("ВНИМАНИЕ: Данные изменились. Откат не сработал корректно.");
            Console.WriteLine();
        }

        /// <summary>
        /// Вспомогательный метод для чтения значения поля водителя
        /// </summary>
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