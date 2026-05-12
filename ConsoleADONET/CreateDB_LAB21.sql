USE master;
GO
IF DB_ID(N'TrafficPoliceDB') IS NOT NULL
    DROP DATABASE TrafficPoliceDB;
GO
CREATE DATABASE TrafficPoliceDB;
GO
USE TrafficPoliceDB;
GO


-- Table 2: Должности (Positions)
CREATE TABLE Positions (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(100) NOT NULL,
    Salary MONEY NOT NULL,
    Responsibilities NVARCHAR(500),
    Requirements NVARCHAR(500)
);
GO

-- Table 3: Звания (Ranks)
CREATE TABLE Ranks (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(100) NOT NULL,
    Allowance MONEY NOT NULL,
    Responsibilities NVARCHAR(500),
    Requirements NVARCHAR(500)
);
GO

-- Table 4: Марки авто (CarBrands)
CREATE TABLE CarBrands (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    Name NVARCHAR(100) NOT NULL,
    Manufacturer NVARCHAR(100),
    CountryOfOrigin NVARCHAR(100),
    ProductionStartDate DATE,
    ProductionEndDate DATE,
    Specifications NVARCHAR(500),
    Category NVARCHAR(50),
    Description NVARCHAR(500)
);
GO

-- Table 5: Водители (Drivers)
CREATE TABLE Drivers (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    FullName NVARCHAR(150) NOT NULL,
    BirthDate DATE,
    Address NVARCHAR(200),
    PassportDetails NVARCHAR(100),
    LicenseNumber NVARCHAR(50) NOT NULL,
    LicenseIssueDate DATE,
    LicenseExpiryDate DATE,
    LicenseCategory NVARCHAR(10),
    Description NVARCHAR(500)
);
GO


-- Table 1: Сотрудники (Employees)
CREATE TABLE Employees (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    FullName NVARCHAR(150) NOT NULL,
    Age INT CHECK (Age >= 18),
    Gender NVARCHAR(10),
    Address NVARCHAR(200),
    Phone NVARCHAR(20),
    PassportDetails NVARCHAR(100),
    PositionId INT NOT NULL,
    RankId INT NOT NULL,
    CONSTRAINT FK_Employees_Positions FOREIGN KEY (PositionId) REFERENCES Positions(Id),
    CONSTRAINT FK_Employees_Ranks FOREIGN KEY (RankId) REFERENCES Ranks(Id)
);
GO

-- Table 6: Авто (Cars)
CREATE TABLE Cars (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    DriverId INT NOT NULL,
    BrandId INT NOT NULL,
    RegistrationNumber NVARCHAR(20) NOT NULL,
    BodyNumber NVARCHAR(50),
    EngineNumber NVARCHAR(50),
    TechPassportNumber NVARCHAR(50),
    ManufactureDate DATE,
    RegistrationDate DATE,
    Color NVARCHAR(50),
    TechInspectionStatus NVARCHAR(50), -- Технический осмотр (статус)
    TechInspectionDate DATE,
    Description NVARCHAR(500),
    RegisteringEmployeeId INT NOT NULL,
    CONSTRAINT FK_Cars_Drivers FOREIGN KEY (DriverId) REFERENCES Drivers(Id),
    CONSTRAINT FK_Cars_CarBrands FOREIGN KEY (BrandId) REFERENCES CarBrands(Id),
    CONSTRAINT FK_Cars_Employees FOREIGN KEY (RegisteringEmployeeId) REFERENCES Employees(Id)
);
GO

-- Table 7: Авто в угоне (StolenCars)
CREATE TABLE StolenCars (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    TheftDate DATETIME NOT NULL,
    ReportDate DATETIME NOT NULL,
    CarId INT NOT NULL,
    TheftCircumstances NVARCHAR(500),
    IsFound BIT NOT NULL DEFAULT 0, -- Отметка о нахождении в угоне (0 - не найдено, 1 - найдено)
    FoundDate DATE,
    RegisteringEmployeeId INT NOT NULL,
    CONSTRAINT FK_StolenCars_Cars FOREIGN KEY (CarId) REFERENCES Cars(Id),
    CONSTRAINT FK_StolenCars_Employees FOREIGN KEY (RegisteringEmployeeId) REFERENCES Employees(Id)
);
GO


IF OBJECT_ID(N'View_CarFullInfo', N'V') IS NOT NULL
    DROP VIEW View_CarFullInfo;
GO
CREATE VIEW View_CarFullInfo AS
SELECT 
    c.Id AS CarId,
    c.RegistrationNumber,
    c.Color,
    c.TechInspectionStatus,
    c.TechInspectionDate,
    c.ManufactureDate,
    d.Id AS DriverId,
    d.FullName AS DriverName,
    d.LicenseNumber,
    cb.Name AS BrandName,
    cb.Manufacturer,
    e.FullName AS RegisteringEmployeeName
FROM Cars c
INNER JOIN Drivers d ON c.DriverId = d.Id
INNER JOIN CarBrands cb ON c.BrandId = cb.Id
INNER JOIN Employees e ON c.RegisteringEmployeeId = e.Id;
GO

-- 1. Positions
IF OBJECT_ID(N'uspInsertPosition', N'P') IS NOT NULL DROP PROCEDURE uspInsertPosition;
GO
CREATE PROCEDURE uspInsertPosition
    @Name NVARCHAR(100),
    @Salary MONEY,
    @Responsibilities NVARCHAR(500) = NULL,
    @Requirements NVARCHAR(500) = NULL,
    @NewId INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO Positions (Name, Salary, Responsibilities, Requirements)
        VALUES (@Name, @Salary, @Responsibilities, @Requirements);
        SET @NewId = SCOPE_IDENTITY();
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END;
GO

-- 2. Ranks
IF OBJECT_ID(N'uspInsertRank', N'P') IS NOT NULL DROP PROCEDURE uspInsertRank;
GO
CREATE PROCEDURE uspInsertRank
    @Name NVARCHAR(100),
    @Allowance MONEY,
    @Responsibilities NVARCHAR(500) = NULL,
    @Requirements NVARCHAR(500) = NULL,
    @NewId INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO Ranks (Name, Allowance, Responsibilities, Requirements)
        VALUES (@Name, @Allowance, @Responsibilities, @Requirements);
        SET @NewId = SCOPE_IDENTITY();
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END;
GO

-- 3. CarBrands
IF OBJECT_ID(N'uspInsertCarBrand', N'P') IS NOT NULL DROP PROCEDURE uspInsertCarBrand;
GO
CREATE PROCEDURE uspInsertCarBrand
    @Name NVARCHAR(100),
    @Manufacturer NVARCHAR(100) = NULL,
    @CountryOfOrigin NVARCHAR(100) = NULL,
    @ProductionStartDate DATE = NULL,
    @ProductionEndDate DATE = NULL,
    @Specifications NVARCHAR(500) = NULL,
    @Category NVARCHAR(50) = NULL,
    @Description NVARCHAR(500) = NULL,
    @NewId INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO CarBrands (Name, Manufacturer, CountryOfOrigin, ProductionStartDate, ProductionEndDate, Specifications, Category, Description)
        VALUES (@Name, @Manufacturer, @CountryOfOrigin, @ProductionStartDate, @ProductionEndDate, @Specifications, @Category, @Description);
        SET @NewId = SCOPE_IDENTITY();
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END;
GO

-- 4. Drivers
IF OBJECT_ID(N'uspInsertDriver', N'P') IS NOT NULL DROP PROCEDURE uspInsertDriver;
GO
CREATE PROCEDURE uspInsertDriver
    @FullName NVARCHAR(150),
    @BirthDate DATE = NULL,
    @Address NVARCHAR(200) = NULL,
    @PassportDetails NVARCHAR(100) = NULL,
    @LicenseNumber NVARCHAR(50),
    @LicenseIssueDate DATE = NULL,
    @LicenseExpiryDate DATE = NULL,
    @LicenseCategory NVARCHAR(10) = NULL,
    @Description NVARCHAR(500) = NULL,
    @NewId INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO Drivers (FullName, BirthDate, Address, PassportDetails, LicenseNumber, LicenseIssueDate, LicenseExpiryDate, LicenseCategory, Description)
        VALUES (@FullName, @BirthDate, @Address, @PassportDetails, @LicenseNumber, @LicenseIssueDate, @LicenseExpiryDate, @LicenseCategory, @Description);
        SET @NewId = SCOPE_IDENTITY();
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END;
GO

-- 5. Employees
IF OBJECT_ID(N'uspInsertEmployee', N'P') IS NOT NULL DROP PROCEDURE uspInsertEmployee;
GO
CREATE PROCEDURE uspInsertEmployee
    @FullName NVARCHAR(150),
    @Age INT,
    @Gender NVARCHAR(10) = NULL,
    @Address NVARCHAR(200) = NULL,
    @Phone NVARCHAR(20) = NULL,
    @PassportDetails NVARCHAR(100) = NULL,
    @PositionId INT,
    @RankId INT,
    @NewId INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO Employees (FullName, Age, Gender, Address, Phone, PassportDetails, PositionId, RankId)
        VALUES (@FullName, @Age, @Gender, @Address, @Phone, @PassportDetails, @PositionId, @RankId);
        SET @NewId = SCOPE_IDENTITY();
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END;
GO

-- 6. Cars
IF OBJECT_ID(N'uspInsertCar', N'P') IS NOT NULL DROP PROCEDURE uspInsertCar;
GO
CREATE PROCEDURE uspInsertCar
    @DriverId INT,
    @BrandId INT,
    @RegistrationNumber NVARCHAR(20),
    @BodyNumber NVARCHAR(50) = NULL,
    @EngineNumber NVARCHAR(50) = NULL,
    @TechPassportNumber NVARCHAR(50) = NULL,
    @ManufactureDate DATE = NULL,
    @RegistrationDate DATE = NULL,
    @Color NVARCHAR(50) = NULL,
    @TechInspectionStatus NVARCHAR(50) = NULL,
    @TechInspectionDate DATE = NULL,
    @Description NVARCHAR(500) = NULL,
    @RegisteringEmployeeId INT,
    @NewId INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO Cars (DriverId, BrandId, RegistrationNumber, BodyNumber, EngineNumber, TechPassportNumber, ManufactureDate, RegistrationDate, Color, TechInspectionStatus, TechInspectionDate, Description, RegisteringEmployeeId)
        VALUES (@DriverId, @BrandId, @RegistrationNumber, @BodyNumber, @EngineNumber, @TechPassportNumber, @ManufactureDate, @RegistrationDate, @Color, @TechInspectionStatus, @TechInspectionDate, @Description, @RegisteringEmployeeId);
        SET @NewId = SCOPE_IDENTITY();
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END;
GO

-- 7. StolenCars
IF OBJECT_ID(N'uspInsertStolenCar', N'P') IS NOT NULL DROP PROCEDURE uspInsertStolenCar;
GO
CREATE PROCEDURE uspInsertStolenCar
    @TheftDate DATETIME,
    @ReportDate DATETIME,
    @CarId INT,
    @TheftCircumstances NVARCHAR(500) = NULL,
    @IsFound BIT = 0,
    @FoundDate DATE = NULL,
    @RegisteringEmployeeId INT,
    @NewId INT OUTPUT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        INSERT INTO StolenCars (TheftDate, ReportDate, CarId, TheftCircumstances, IsFound, FoundDate, RegisteringEmployeeId)
        VALUES (@TheftDate, @ReportDate, @CarId, @TheftCircumstances, @IsFound, @FoundDate, @RegisteringEmployeeId);
        SET @NewId = SCOPE_IDENTITY();
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END;
GO

-- Запрос 2: Список автомобилей с просроченным техосмотром
IF OBJECT_ID(N'uspGetExpiredTechInspections', N'P') IS NOT NULL DROP PROCEDURE uspGetExpiredTechInspections;
GO
CREATE PROCEDURE uspGetExpiredTechInspections
AS
BEGIN
    SET NOCOUNT ON;
    SELECT v.* 
    FROM View_CarFullInfo v
    WHERE v.TechInspectionDate < CAST(GETDATE() AS DATE) 
      AND v.TechInspectionDate IS NOT NULL;
END;
GO

-- Запрос 3: Список угонов за прошлый месяц
IF OBJECT_ID(N'uspGetTheftsLastMonth', N'P') IS NOT NULL DROP PROCEDURE uspGetTheftsLastMonth;
GO
CREATE PROCEDURE uspGetTheftsLastMonth
AS
BEGIN
    SET NOCOUNT ON;
    DECLARE @StartOfLastMonth DATE = DATEADD(MONTH, DATEDIFF(MONTH, 0, GETDATE()) - 1, 0);
    DECLARE @EndOfLastMonth DATE = DATEADD(DAY, -1, DATEADD(MONTH, 1, @StartOfLastMonth));

    SELECT sc.Id, sc.TheftDate, sc.ReportDate, sc.TheftCircumstances, sc.IsFound, sc.FoundDate,
           c.RegistrationNumber, d.FullName AS DriverName
    FROM StolenCars sc
    JOIN Cars c ON sc.CarId = c.Id
    JOIN Drivers d ON c.DriverId = d.Id
    WHERE sc.TheftDate >= @StartOfLastMonth AND sc.TheftDate <= @EndOfLastMonth;
END;
GO

-- Запрос 4: Параметрический запрос сотрудников по должности
IF OBJECT_ID(N'uspGetEmployeesByPosition', N'P') IS NOT NULL DROP PROCEDURE uspGetEmployeesByPosition;
GO
CREATE PROCEDURE uspGetEmployeesByPosition
    @PositionName NVARCHAR(100) = N''
AS
BEGIN
    SET NOCOUNT ON;
    SELECT e.Id, e.FullName, e.Age, e.Gender, e.Phone, p.Name AS PositionName, r.Name AS RankName
    FROM Employees e
    JOIN Positions p ON e.PositionId = p.Id
    JOIN Ranks r ON e.RankId = r.Id
    WHERE (@PositionName = N'' OR p.Name LIKE @PositionName + N'%');
END;
GO

-- Запрос 5: Параметрический запрос авто по владельцу (водителю)
IF OBJECT_ID(N'uspGetCarsByDriver', N'P') IS NOT NULL DROP PROCEDURE uspGetCarsByDriver;
GO
CREATE PROCEDURE uspGetCarsByDriver
    @DriverName NVARCHAR(150) = N''
AS
BEGIN
    SET NOCOUNT ON;
    SELECT v.*
    FROM View_CarFullInfo v
    WHERE (@DriverName = N'' OR v.DriverName LIKE N'%' + @DriverName + N'%');
END;
GO

-- Запрос 6: Перекрестный запрос (PIVOT) - количество найденных угнанных авто по годам для каждой марки
IF OBJECT_ID(N'uspGetFoundStolenCarsPivot', N'P') IS NOT NULL DROP PROCEDURE uspGetFoundStolenCarsPivot;
GO
CREATE PROCEDURE uspGetFoundStolenCarsPivot
AS
BEGIN
    SET NOCOUNT ON;
    SELECT BrandName, [2022], [2023], [2024], [2025]
    FROM (
        SELECT cb.Name AS BrandName, YEAR(sc.FoundDate) AS FoundYear
        FROM StolenCars sc
        JOIN Cars c ON sc.CarId = c.Id
        JOIN CarBrands cb ON c.BrandId = cb.Id
        WHERE sc.IsFound = 1 AND sc.FoundDate IS NOT NULL
    ) AS SourceTable
    PIVOT (
        COUNT(FoundYear) FOR FoundYear IN ([2022], [2023], [2024], [2025])
    ) AS PivotTable;
END;
GO

-- Запрос 7: Количество автомобилей, прошедших техосмотр, по годам
IF OBJECT_ID(N'uspGetTechInspectionCountByYear', N'P') IS NOT NULL DROP PROCEDURE uspGetTechInspectionCountByYear;
GO
CREATE PROCEDURE uspGetTechInspectionCountByYear
AS
BEGIN
    SET NOCOUNT ON;
    SELECT YEAR(TechInspectionDate) AS InspectionYear, COUNT(Id) AS CarsCount
    FROM Cars
    WHERE TechInspectionStatus = N'Пройден' AND TechInspectionDate IS NOT NULL
    GROUP BY YEAR(TechInspectionDate)
    ORDER BY InspectionYear DESC;
END;
GO

-- Операция 9: Обновление данных о водителе
IF OBJECT_ID(N'uspUpdateDriver', N'P') IS NOT NULL DROP PROCEDURE uspUpdateDriver;
GO
CREATE PROCEDURE uspUpdateDriver
    @Id INT,
    @FullName NVARCHAR(150) = NULL,
    @Address NVARCHAR(200) = NULL,
    @Phone NVARCHAR(20) = NULL,
    @LicenseNumber NVARCHAR(50) = NULL,
    @LicenseExpiryDate DATE = NULL
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        UPDATE Drivers SET
            FullName = ISNULL(@FullName, FullName),
            Address = ISNULL(@Address, Address),
            PassportDetails = ISNULL(@Phone, PassportDetails),
            LicenseNumber = ISNULL(@LicenseNumber, LicenseNumber),
            LicenseExpiryDate = ISNULL(@LicenseExpiryDate, LicenseExpiryDate)
        WHERE Id = @Id;
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END;
GO

-- Операция 10: Удаление данных о водителе
IF OBJECT_ID(N'uspDeleteDriver', N'P') IS NOT NULL DROP PROCEDURE uspDeleteDriver;
GO
CREATE PROCEDURE uspDeleteDriver
    @Id INT
AS
BEGIN
    SET NOCOUNT ON;
    BEGIN TRY
        -- Проверка на связанные записи в Cars (чтобы не нарушить FK)
        IF EXISTS (SELECT 1 FROM Cars WHERE DriverId = @Id)
            THROW 50001, N'Невозможно удалить водителя: за ним закреплены автомобили.', 1;
        
        DELETE FROM Drivers WHERE Id = @Id;
    END TRY
    BEGIN CATCH
        THROW;
    END CATCH
END;
GO