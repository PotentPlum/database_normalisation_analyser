SET NOCOUNT ON;

IF DB_ID('OperationsDemo') IS NOT NULL
BEGIN
    ALTER DATABASE OperationsDemo SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE OperationsDemo;
END;
GO

CREATE DATABASE OperationsDemo;
GO

USE OperationsDemo;
GO

-- Generate a reusable tally table with 2,000,000 rows
IF OBJECT_ID('dbo.Numbers', 'U') IS NOT NULL
    DROP TABLE dbo.Numbers;

;WITH E1(N) AS (SELECT 1 FROM (VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1)) AS d(n)),
      E2(N) AS (SELECT 1 FROM E1 a CROSS JOIN E1 b),                -- 10^2
      E4(N) AS (SELECT 1 FROM E2 a CROSS JOIN E2 b),                -- 10^4
      E5(N) AS (SELECT 1 FROM E4 a CROSS JOIN E2 b),                -- 10^6
      E6(N) AS (SELECT 1 FROM E5 a CROSS JOIN E1 b)                 -- 10^7
SELECT TOP (2000000)
    ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS n
INTO dbo.Numbers
FROM E6;
GO

-- Core master data tables
CREATE TABLE dbo.Customers (
    CustomerID          INT IDENTITY(1,1) PRIMARY KEY,
    CustomerName        NVARCHAR(200) NOT NULL,
    Email               NVARCHAR(320) NOT NULL,
    Phone               NVARCHAR(50) NULL,
    Segment             NVARCHAR(50) NOT NULL,
    CreatedAt           DATETIME2     NOT NULL,
    City                NVARCHAR(100) NULL
);

CREATE TABLE dbo.CustomerAddresses (
    AddressID    INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID   INT NOT NULL,
    AddressType  NVARCHAR(50) NOT NULL,
    Street       NVARCHAR(200) NOT NULL,
    City         NVARCHAR(100) NOT NULL,
    StateProvince NVARCHAR(100) NOT NULL,
    PostalCode   NVARCHAR(20) NOT NULL,
    Country      NVARCHAR(100) NOT NULL,
    IsPrimary    BIT NOT NULL DEFAULT(0),
    CONSTRAINT FK_CustomerAddresses_Customers FOREIGN KEY (CustomerID) REFERENCES dbo.Customers(CustomerID)
);

CREATE TABLE dbo.CustomerContacts (
    ContactID     INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID    INT NOT NULL,
    CustomerName  NVARCHAR(200) NOT NULL, -- denormalized copy to intentionally violate 3NF
    ContactType   NVARCHAR(50) NOT NULL,
    ContactValue  NVARCHAR(200) NOT NULL,
    Notes         NVARCHAR(200) NULL,
    CONSTRAINT FK_CustomerContacts_Customers FOREIGN KEY (CustomerID) REFERENCES dbo.Customers(CustomerID)
);

CREATE TABLE dbo.Suppliers (
    SupplierID     INT IDENTITY(1,1) PRIMARY KEY,
    SupplierName   NVARCHAR(200) NOT NULL,
    City           NVARCHAR(100) NOT NULL,
    Country        NVARCHAR(100) NOT NULL,
    Rating         INT NOT NULL
);

CREATE TABLE dbo.ProductCategories (
    CategoryID    INT IDENTITY(1,1) PRIMARY KEY,
    CategoryName  NVARCHAR(200) NOT NULL
);

CREATE TABLE dbo.Products (
    ProductID     INT IDENTITY(1,1) PRIMARY KEY,
    SKU           NVARCHAR(50) NOT NULL,
    ProductName   NVARCHAR(200) NOT NULL,
    CategoryID    INT NOT NULL,
    SupplierID    INT NOT NULL,
    UnitPrice     DECIMAL(18,2) NOT NULL,
    CONSTRAINT FK_Products_Category FOREIGN KEY (CategoryID) REFERENCES dbo.ProductCategories(CategoryID),
    CONSTRAINT FK_Products_Supplier FOREIGN KEY (SupplierID) REFERENCES dbo.Suppliers(SupplierID)
);

CREATE TABLE dbo.ProductCategoryAssignments (
    AssignmentID INT IDENTITY(1,1) PRIMARY KEY,
    ProductID    INT NOT NULL,
    CategoryID   INT NOT NULL,
    AssignedOn   DATETIME2 NOT NULL,
    CONSTRAINT FK_CategoryAssignments_Product FOREIGN KEY (ProductID) REFERENCES dbo.Products(ProductID),
    CONSTRAINT FK_CategoryAssignments_Category FOREIGN KEY (CategoryID) REFERENCES dbo.ProductCategories(CategoryID)
);

CREATE TABLE dbo.Warehouses (
    WarehouseID    INT IDENTITY(1,1) PRIMARY KEY,
    WarehouseName  NVARCHAR(200) NOT NULL,
    City           NVARCHAR(100) NOT NULL,
    Region         NVARCHAR(100) NOT NULL
);

CREATE TABLE dbo.InventoryLevels (
    WarehouseID INT NOT NULL,
    ProductID   INT NOT NULL,
    OnHand      INT NOT NULL,
    LastCounted DATETIME2 NOT NULL,
    CONSTRAINT PK_InventoryLevels PRIMARY KEY (WarehouseID, ProductID),
    CONSTRAINT FK_InventoryLevels_Warehouse FOREIGN KEY (WarehouseID) REFERENCES dbo.Warehouses(WarehouseID),
    CONSTRAINT FK_InventoryLevels_Product FOREIGN KEY (ProductID) REFERENCES dbo.Products(ProductID)
);

CREATE TABLE dbo.InventorySnapshots (
    SnapshotID     INT IDENTITY(1,1) PRIMARY KEY,
    WarehouseID    INT NOT NULL,
    WarehouseName  NVARCHAR(200) NOT NULL, -- denormalized warehouse name
    ProductID      INT NOT NULL,
    SnapshotDate   DATETIME2 NOT NULL,
    OnHand         INT NOT NULL,
    CountedBy      NVARCHAR(200) NOT NULL,
    CONSTRAINT FK_InventorySnapshots_Warehouse FOREIGN KEY (WarehouseID) REFERENCES dbo.Warehouses(WarehouseID),
    CONSTRAINT FK_InventorySnapshots_Product FOREIGN KEY (ProductID) REFERENCES dbo.Products(ProductID)
);

CREATE TABLE dbo.PurchaseOrders (
    PurchaseOrderID INT IDENTITY(1,1) PRIMARY KEY,
    SupplierID      INT NOT NULL,
    OrderedDate     DATETIME2 NOT NULL,
    ExpectedDate    DATETIME2 NOT NULL,
    Status          NVARCHAR(50) NOT NULL,
    BuyerNote       NVARCHAR(200) NULL,
    CONSTRAINT FK_PurchaseOrders_Supplier FOREIGN KEY (SupplierID) REFERENCES dbo.Suppliers(SupplierID)
);

CREATE TABLE dbo.PurchaseOrderLines (
    PurchaseOrderLineID INT IDENTITY(1,1) PRIMARY KEY,
    PurchaseOrderID     INT NOT NULL,
    ProductID           INT NOT NULL,
    Quantity            INT NOT NULL,
    UnitCost            DECIMAL(18,2) NOT NULL,
    WarehouseID         INT NOT NULL,
    CONSTRAINT FK_PurchaseOrderLines_PO FOREIGN KEY (PurchaseOrderID) REFERENCES dbo.PurchaseOrders(PurchaseOrderID),
    CONSTRAINT FK_PurchaseOrderLines_Product FOREIGN KEY (ProductID) REFERENCES dbo.Products(ProductID),
    CONSTRAINT FK_PurchaseOrderLines_Warehouse FOREIGN KEY (WarehouseID) REFERENCES dbo.Warehouses(WarehouseID)
);

CREATE TABLE dbo.SalesOrders (
    SalesOrderID    INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID      INT NOT NULL,
    OrderDate       DATETIME2 NOT NULL,
    SalesChannel    NVARCHAR(50) NOT NULL,
    CustomerCity    NVARCHAR(100) NULL, -- denormalized to break 3NF intentionally
    CustomerSegment NVARCHAR(50) NULL,
    Status          NVARCHAR(50) NOT NULL,
    CONSTRAINT FK_SalesOrders_Customers FOREIGN KEY (CustomerID) REFERENCES dbo.Customers(CustomerID)
);

CREATE TABLE dbo.SalesOrderLines (
    SalesOrderLineID INT IDENTITY(1,1) PRIMARY KEY,
    SalesOrderID     INT NOT NULL,
    ProductID        INT NOT NULL,
    Quantity         INT NOT NULL,
    UnitPrice        DECIMAL(18,2) NOT NULL,
    DiscountPct      DECIMAL(5,4) NOT NULL,
    FulfillmentSite  NVARCHAR(200) NOT NULL,
    CONSTRAINT FK_SalesOrderLines_Order FOREIGN KEY (SalesOrderID) REFERENCES dbo.SalesOrders(SalesOrderID),
    CONSTRAINT FK_SalesOrderLines_Product FOREIGN KEY (ProductID) REFERENCES dbo.Products(ProductID)
);

CREATE TABLE dbo.Invoices (
    InvoiceID       INT IDENTITY(1,1) PRIMARY KEY,
    SalesOrderID    INT NOT NULL,
    InvoiceDate     DATETIME2 NOT NULL,
    DueDate         DATETIME2 NOT NULL,
    Status          NVARCHAR(50) NOT NULL,
    CustomerName    NVARCHAR(200) NOT NULL, -- denormalized copy to force 3NF issues
    CONSTRAINT FK_Invoices_SalesOrder FOREIGN KEY (SalesOrderID) REFERENCES dbo.SalesOrders(SalesOrderID)
);

CREATE TABLE dbo.InvoiceLines (
    InvoiceLineID  INT IDENTITY(1,1) PRIMARY KEY,
    InvoiceID      INT NOT NULL,
    ProductID      INT NOT NULL,
    Quantity       INT NOT NULL,
    UnitPrice      DECIMAL(18,2) NOT NULL,
    SalesOrderLineID INT NOT NULL,
    CONSTRAINT FK_InvoiceLines_Invoice FOREIGN KEY (InvoiceID) REFERENCES dbo.Invoices(InvoiceID),
    CONSTRAINT FK_InvoiceLines_Product FOREIGN KEY (ProductID) REFERENCES dbo.Products(ProductID),
    CONSTRAINT FK_InvoiceLines_OrderLine FOREIGN KEY (SalesOrderLineID) REFERENCES dbo.SalesOrderLines(SalesOrderLineID)
);

CREATE TABLE dbo.Payments (
    PaymentID   INT IDENTITY(1,1) PRIMARY KEY,
    InvoiceID   INT NOT NULL,
    Amount      DECIMAL(18,2) NOT NULL,
    PaymentDate DATETIME2 NOT NULL,
    Method      NVARCHAR(50) NOT NULL,
    Reference   NVARCHAR(100) NULL,
    CONSTRAINT FK_Payments_Invoice FOREIGN KEY (InvoiceID) REFERENCES dbo.Invoices(InvoiceID)
);

CREATE TABLE dbo.Shipments (
    ShipmentID    INT IDENTITY(1,1) PRIMARY KEY,
    SalesOrderID  INT NOT NULL,
    WarehouseID   INT NOT NULL,
    ShippedDate   DATETIME2 NOT NULL,
    Carrier       NVARCHAR(100) NOT NULL,
    TrackingCode  NVARCHAR(100) NOT NULL,
    CONSTRAINT FK_Shipments_Order FOREIGN KEY (SalesOrderID) REFERENCES dbo.SalesOrders(SalesOrderID),
    CONSTRAINT FK_Shipments_Warehouse FOREIGN KEY (WarehouseID) REFERENCES dbo.Warehouses(WarehouseID)
);

CREATE TABLE dbo.ShipmentItems (
    ShipmentItemID  INT IDENTITY(1,1) PRIMARY KEY,
    ShipmentID      INT NOT NULL,
    SalesOrderLineID INT NOT NULL,
    Quantity        INT NOT NULL,
    CONSTRAINT FK_ShipmentItems_Shipment FOREIGN KEY (ShipmentID) REFERENCES dbo.Shipments(ShipmentID),
    CONSTRAINT FK_ShipmentItems_OrderLine FOREIGN KEY (SalesOrderLineID) REFERENCES dbo.SalesOrderLines(SalesOrderLineID)
);

CREATE TABLE dbo.MarketingInteractions (
    InteractionID  INT IDENTITY(1,1) PRIMARY KEY,
    CustomerID     INT NOT NULL,
    CampaignName   NVARCHAR(200) NOT NULL,
    Channel        NVARCHAR(100) NOT NULL,
    Responded      BIT NOT NULL,
    ResponseDate   DATETIME2 NULL,
    OfferCode      NVARCHAR(100) NULL,
    CustomerEmail  NVARCHAR(320) NOT NULL, -- denormalized for 3NF testing
    CONSTRAINT FK_MarketingInteractions_Customer FOREIGN KEY (CustomerID) REFERENCES dbo.Customers(CustomerID)
);
GO

-- Seed reference data
INSERT INTO dbo.ProductCategories (CategoryName)
VALUES
(N'Electronics'), (N'Home & Garden'), (N'Office Supplies'), (N'Industrial'), (N'Automotive'),
(N'Health & Beauty'), (N'Sports'), (N'Fashion'), (N'Toys'), (N'Groceries');

INSERT INTO dbo.Suppliers (SupplierName, City, Country, Rating)
SELECT TOP (100)
    CONCAT(N'Supplier ', n),
    CONCAT(N'City ', n % 150),
    CASE WHEN n % 3 = 0 THEN N'USA' WHEN n % 3 = 1 THEN N'Germany' ELSE N'Canada' END,
    (n % 5) + 1
FROM dbo.Numbers
ORDER BY n;

INSERT INTO dbo.Warehouses (WarehouseName, City, Region)
SELECT TOP (25)
    CONCAT(N'Warehouse ', n),
    CONCAT(N'City ', n % 100),
    CASE WHEN n % 4 = 0 THEN N'North' WHEN n % 4 = 1 THEN N'East' WHEN n % 4 = 2 THEN N'South' ELSE N'West' END
FROM dbo.Numbers
ORDER BY n;

INSERT INTO dbo.Customers (CustomerName, Email, Phone, Segment, CreatedAt, City)
SELECT TOP (50000)
    CONCAT(N'Customer ', RIGHT('000000' + CAST(n AS NVARCHAR(6)), 6)),
    CONCAT(N'customer', n, N'@example.com'),
    CONCAT(N'+1-555-', RIGHT('0000' + CAST(n % 10000 AS NVARCHAR(4)), 4)),
    CASE WHEN n % 5 = 0 THEN N'Enterprise' WHEN n % 5 = 1 THEN N'SMB' WHEN n % 5 = 2 THEN N'Consumer' WHEN n % 5 = 3 THEN N'Partner' ELSE N'Education' END,
    DATEADD(DAY, -(n % 3650), SYSUTCDATETIME()),
    CONCAT(N'City ', n % 200)
FROM dbo.Numbers
ORDER BY n;

INSERT INTO dbo.CustomerAddresses (CustomerID, AddressType, Street, City, StateProvince, PostalCode, Country, IsPrimary)
SELECT TOP (50000)
    ((n - 1) % 50000) + 1,
    CASE WHEN n % 2 = 0 THEN N'Billing' ELSE N'Shipping' END,
    CONCAT(N'Street ', n),
    CONCAT(N'City ', n % 200),
    CONCAT(N'State ', n % 50),
    RIGHT('00000' + CAST(n % 100000 AS NVARCHAR(5)), 5),
    CASE WHEN n % 3 = 0 THEN N'USA' WHEN n % 3 = 1 THEN N'Canada' ELSE N'UK' END,
    CASE WHEN n % 2 = 0 THEN 1 ELSE 0 END
FROM dbo.Numbers
ORDER BY n;

INSERT INTO dbo.CustomerContacts (CustomerID, CustomerName, ContactType, ContactValue, Notes)
SELECT TOP (75000)
    ((n - 1) % 50000) + 1,
    CONCAT(N'Customer ', RIGHT('000000' + CAST(((n - 1) % 50000) + 1 AS NVARCHAR(6)), 6)),
    CASE WHEN n % 3 = 0 THEN N'Email' WHEN n % 3 = 1 THEN N'Phone' ELSE N'SMS' END,
    CONCAT(N'contact', n, CASE WHEN n % 3 = 0 THEN N'@example.com' ELSE N'' END),
    CASE WHEN n % 4 = 0 THEN N'Prefers morning calls' ELSE N'' END
FROM dbo.Numbers
ORDER BY n;

INSERT INTO dbo.Products (SKU, ProductName, CategoryID, SupplierID, UnitPrice)
SELECT TOP (1000)
    CONCAT(N'SKU', RIGHT('00000' + CAST(n AS NVARCHAR(5)), 5)),
    CONCAT(N'Product ', n),
    ((n - 1) % 10) + 1,
    ((n - 1) % 100) + 1,
    CAST(ROUND(((n % 200) * 1.35) + 5, 2) AS DECIMAL(18,2))
FROM dbo.Numbers
ORDER BY n;

INSERT INTO dbo.ProductCategoryAssignments (ProductID, CategoryID, AssignedOn)
SELECT TOP (2000)
    ((n - 1) % 1000) + 1,
    ((n - 1) % 10) + 1,
    DATEADD(DAY, -(n % 400), SYSUTCDATETIME())
FROM dbo.Numbers
ORDER BY n;

INSERT INTO dbo.InventoryLevels (WarehouseID, ProductID, OnHand, LastCounted)
SELECT TOP (20000)
    ((n - 1) % 25) + 1,
    ((n - 1) % 1000) + 1,
    (n % 200) + 20,
    DATEADD(DAY, -(n % 90), SYSUTCDATETIME())
FROM dbo.Numbers
ORDER BY n;

INSERT INTO dbo.InventorySnapshots (WarehouseID, WarehouseName, ProductID, SnapshotDate, OnHand, CountedBy)
SELECT TOP (50000)
    ((n - 1) % 25) + 1,
    CONCAT(N'Warehouse ', ((n - 1) % 25) + 1),
    ((n - 1) % 1000) + 1,
    DATEADD(DAY, -(n % 365), SYSUTCDATETIME()),
    (n % 250) + 10,
    CONCAT(N'Employee ', n % 300)
FROM dbo.Numbers
ORDER BY n;

INSERT INTO dbo.PurchaseOrders (SupplierID, OrderedDate, ExpectedDate, Status, BuyerNote)
SELECT TOP (8000)
    ((n - 1) % 100) + 1,
    DATEADD(DAY, -(n % 200), SYSUTCDATETIME()),
    DATEADD(DAY, (n % 15), SYSUTCDATETIME()),
    CASE WHEN n % 3 = 0 THEN N'Sent' WHEN n % 3 = 1 THEN N'Confirmed' ELSE N'Received' END,
    CONCAT(N'PO note ', n)
FROM dbo.Numbers
ORDER BY n;

INSERT INTO dbo.PurchaseOrderLines (PurchaseOrderID, ProductID, Quantity, UnitCost, WarehouseID)
SELECT TOP (60000)
    ((n - 1) % 8000) + 1,
    ((n - 1) % 1000) + 1,
    (n % 50) + 1,
    CAST(ROUND(((n % 120) * 1.2) + 3, 2) AS DECIMAL(18,2)),
    ((n - 1) % 25) + 1
FROM dbo.Numbers
ORDER BY n;

INSERT INTO dbo.SalesOrders (CustomerID, OrderDate, SalesChannel, CustomerCity, CustomerSegment, Status)
SELECT TOP (200000)
    ((n - 1) % 50000) + 1,
    DATEADD(DAY, -(n % 730), SYSUTCDATETIME()),
    CASE WHEN n % 3 = 0 THEN N'Online' WHEN n % 3 = 1 THEN N'Retail' ELSE N'Partner' END,
    CONCAT(N'City ', ((n - 1) % 200)),
    CASE WHEN n % 5 = 0 THEN N'Enterprise' WHEN n % 5 = 1 THEN N'SMB' WHEN n % 5 = 2 THEN N'Consumer' WHEN n % 5 = 3 THEN N'Partner' ELSE N'Education' END,
    CASE WHEN n % 4 = 0 THEN N'Shipped' WHEN n % 4 = 1 THEN N'Processing' WHEN n % 4 = 2 THEN N'Delivered' ELSE N'On Hold' END
FROM dbo.Numbers
ORDER BY n;

INSERT INTO dbo.SalesOrderLines (SalesOrderID, ProductID, Quantity, UnitPrice, DiscountPct, FulfillmentSite)
SELECT TOP (1200000)
    ((n - 1) % 200000) + 1,
    ((n - 1) % 1000) + 1,
    (n % 8) + 1,
    CAST(ROUND(((n % 250) * 1.15) + 5, 2) AS DECIMAL(18,2)),
    (n % 20) * 0.005,
    CONCAT(N'Warehouse ', ((n - 1) % 25) + 1)
FROM dbo.Numbers
ORDER BY n;

INSERT INTO dbo.Invoices (SalesOrderID, InvoiceDate, DueDate, Status, CustomerName)
SELECT
    so.SalesOrderID,
    DATEADD(DAY, 1, so.OrderDate),
    DATEADD(DAY, 30, so.OrderDate),
    CASE WHEN so.SalesOrderID % 4 = 0 THEN N'Sent' WHEN so.SalesOrderID % 4 = 1 THEN N'Open' WHEN so.SalesOrderID % 4 = 2 THEN N'Paid' ELSE N'Overdue' END,
    c.CustomerName
FROM dbo.SalesOrders AS so
JOIN dbo.Customers AS c ON so.CustomerID = c.CustomerID;

INSERT INTO dbo.InvoiceLines (InvoiceID, ProductID, Quantity, UnitPrice, SalesOrderLineID)
SELECT TOP (1200000)
    ((n - 1) % 200000) + 1,
    ((n - 1) % 1000) + 1,
    (n % 10) + 1,
    CAST(ROUND(((n % 200) * 1.35) + 6, 2) AS DECIMAL(18,2)),
    ((n - 1) % 1200000) + 1
FROM dbo.Numbers
ORDER BY n;

INSERT INTO dbo.Payments (InvoiceID, Amount, PaymentDate, Method, Reference)
SELECT TOP (200000)
    ((n - 1) % 200000) + 1,
    CAST(ROUND(((n % 300) * 1.25) + 15, 2) AS DECIMAL(18,2)),
    DATEADD(DAY, (n % 45), SYSUTCDATETIME()),
    CASE WHEN n % 3 = 0 THEN N'Credit Card' WHEN n % 3 = 1 THEN N'Wire' ELSE N'Check' END,
    CONCAT(N'TXN', RIGHT('000000' + CAST(n AS NVARCHAR(6)), 6))
FROM dbo.Numbers
ORDER BY n;

INSERT INTO dbo.Shipments (SalesOrderID, WarehouseID, ShippedDate, Carrier, TrackingCode)
SELECT TOP (180000)
    ((n - 1) % 200000) + 1,
    ((n - 1) % 25) + 1,
    DATEADD(DAY, (n % 20), SYSUTCDATETIME()),
    CASE WHEN n % 3 = 0 THEN N'UPS' WHEN n % 3 = 1 THEN N'FedEx' ELSE N'DHL' END,
    CONCAT(N'TRK', RIGHT('00000000' + CAST(n AS NVARCHAR(8)), 8))
FROM dbo.Numbers
ORDER BY n;

INSERT INTO dbo.ShipmentItems (ShipmentID, SalesOrderLineID, Quantity)
SELECT TOP (1200000)
    ((n - 1) % 180000) + 1,
    ((n - 1) % 1200000) + 1,
    (n % 6) + 1
FROM dbo.Numbers
ORDER BY n;

INSERT INTO dbo.MarketingInteractions (CustomerID, CampaignName, Channel, Responded, ResponseDate, OfferCode, CustomerEmail)
SELECT TOP (1500000)
    ((n - 1) % 50000) + 1,
    CONCAT(N'Campaign ', (n % 40)),
    CASE WHEN n % 4 = 0 THEN N'Email' WHEN n % 4 = 1 THEN N'Social' WHEN n % 4 = 2 THEN N'Paid Search' ELSE N'Affiliate' END,
    CASE WHEN n % 3 = 0 THEN 1 ELSE 0 END,
    CASE WHEN n % 3 = 0 THEN DATEADD(DAY, -(n % 90), SYSUTCDATETIME()) ELSE NULL END,
    CONCAT(N'OFFER', RIGHT('0000' + CAST(n % 5000 AS NVARCHAR(4)), 4)),
    CONCAT(N'customer', ((n - 1) % 50000) + 1, N'@example.com')
FROM dbo.Numbers
ORDER BY n;

PRINT 'OperationsDemo database created with 20 interrelated tables and rich sample data.';
