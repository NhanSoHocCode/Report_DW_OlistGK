-- ============================================================
-- KHỞI TẠO DATABASE
-- ============================================================
USE master
GO

IF NOT EXISTS (SELECT name FROM sys.databases WHERE name = 'Olist1DW')
BEGIN
    CREATE DATABASE Olist1DW
END
GO

USE Olist1DW
GO

-- ============================================================
-- XÓA CÁC BẢNG NẾU ĐÃ TỒN TẠI (Theo thứ tự)
-- ============================================================
IF OBJECT_ID('FACT_REVIEW',     'U') IS NOT NULL DROP TABLE FACT_REVIEW;
IF OBJECT_ID('FACT_DELIVERY',   'U') IS NOT NULL DROP TABLE FACT_DELIVERY;
IF OBJECT_ID('FACT_PAYMENT',    'U') IS NOT NULL DROP TABLE FACT_PAYMENT;
IF OBJECT_ID('FACT_ORDER_ITEM', 'U') IS NOT NULL DROP TABLE FACT_ORDER_ITEM;
IF OBJECT_ID('DIM_PRODUCT',     'U') IS NOT NULL DROP TABLE DIM_PRODUCT;
IF OBJECT_ID('DIM_CUSTOMER',    'U') IS NOT NULL DROP TABLE DIM_CUSTOMER;
IF OBJECT_ID('DIM_SELLER',      'U') IS NOT NULL DROP TABLE DIM_SELLER;
IF OBJECT_ID('DIM_GEOLOCATION', 'U') IS NOT NULL DROP TABLE DIM_GEOLOCATION;
IF OBJECT_ID('DIM_CATEGORY',    'U') IS NOT NULL DROP TABLE DIM_CATEGORY;
IF OBJECT_ID('DIM_PAYMENT_TYPE','U') IS NOT NULL DROP TABLE DIM_PAYMENT_TYPE;
GO

-- ============================================================
-- DIMENSION TABLES
-- ============================================================

CREATE TABLE DIM_GEOLOCATION (
    LocationKey      INT            IDENTITY(1,1) PRIMARY KEY,
    ZipCodePrefix    NVARCHAR(20)   NOT NULL,
    GeoLocationLat   DECIMAL(10, 6) NULL,
    GeoLocationLng   DECIMAL(10, 6) NULL,
    CityKey          INT            NULL
);
GO

CREATE TABLE DIM_CATEGORY (
    CategoryKey         INT            IDENTITY(1,1) PRIMARY KEY,
    CategoryName        NVARCHAR(200)  NOT NULL,
    CategoryNameEnglish NVARCHAR(200)  NULL
);
GO

CREATE TABLE DIM_PRODUCT (
    ProductKey      INT             IDENTITY(1,1) PRIMARY KEY,
    CategoryKey     INT             NULL,
	ProductID		NVARCHAR(255)	NOT NULL,
    ProductWeight   DECIMAL(10, 2)  NULL,
    RowIsCurrent    BIT             DEFAULT 1,
    RowStartDate    DATE            NOT NULL,
    RowEndDate      DATE            NULL,
    RowChangeReason NVARCHAR(200)   NULL
);
GO

CREATE TABLE DIM_SELLER (
    SellerKey       INT           IDENTITY(1,1) PRIMARY KEY,
	SellerID        VARCHAR(100)  NOT NULL,
    LocationKey     INT           NULL,
    RowIsCurrent    BIT           DEFAULT 1,
    RowStartDate    DATE          NOT NULL,
    RowEndDate      DATE          NULL,
    RowChangeReason NVARCHAR(200)  NULL
);
GO

CREATE TABLE DIM_CUSTOMER (
    CustomerKey     INT           IDENTITY(1,1) PRIMARY KEY,
    CustomerID      VARCHAR(100)  NOT NULL,
    LocationKey     INT           NULL,
    RowIsCurrent    BIT           DEFAULT 1,
    RowStartDate    DATE          NOT NULL,
    RowEndDate      DATE          NULL,
    RowChangeReason NVARCHAR(200)  NULL
);
GO

CREATE TABLE DIM_PAYMENT_TYPE (
    PaymentTypeKey  INT           NOT NULL PRIMARY KEY,
    PaymentType     VARCHAR(50)   NOT NULL
);
GO

-- ============================================================
-- FACT TABLES
-- ============================================================

CREATE TABLE FACT_ORDER_ITEM (
	OrderItemKey    INT             IDENTITY(1,1) PRIMARY KEY,
    OrderID         VARCHAR(50)     NOT NULL,
    OrderItemID     INT             NOT NULL,
    ProductKey      INT             NOT NULL,
    CustomerKey     INT             NOT NULL,
    SellerKey       INT             NOT NULL,
    OrderDateKey    INT             NOT NULL,
    ShippingDateKey INT             NOT NULL,
    TotalPrice      DECIMAL(18, 2)  NOT NULL DEFAULT 0,
    FreightValue    DECIMAL(18, 2)  NOT NULL DEFAULT 0
);
GO

CREATE TABLE FACT_PAYMENT (
    PaymentKey           INT             IDENTITY(1,1) PRIMARY KEY,
    OrderID              VARCHAR(50)     NOT NULL,
    CustomerKey          INT             NOT NULL,
    DateKey              INT             NOT NULL,
    SellerKey            INT             NOT NULL,
    PaymentTypeKey       INT             NOT NULL,
    PaymentValue         DECIMAL(18, 2)  NOT NULL DEFAULT 0,
    PaymentInstallments  INT             NOT NULL DEFAULT 1
);
GO

CREATE TABLE FACT_DELIVERY (
	DeliveryKey           INT			 IDENTITY(1,1) PRIMARY KEY,
    OrderID               VARCHAR(50)	 NOT NULL,
    CustomerKey           INT			 NOT NULL,
    SellerKey             INT			 NOT NULL,
    DatePurchaseKey       INT			 NOT NULL,
    DateDeliveredKey      INT			 NOT NULL,
    ActualDeliveryDays    INT			 NULL,
    IsLate                BIT			 NOT NULL DEFAULT 0
);
GO

CREATE TABLE FACT_REVIEW (
	ReviewKey                 INT             IDENTITY(1,1) PRIMARY KEY,
    ReviewID                  VARCHAR(100)    NOT NULL,
    OrderID                   VARCHAR(50)     NOT NULL,
    CustomerKey               INT             NOT NULL,
    ProductKey                INT             NOT NULL,
    ReviewScore               INT             NULL,
    ResponseTime              DECIMAL(10, 2)  NULL
);
GO

-- POPULATE DATA
INSERT INTO DIM_PAYMENT_TYPE (PaymentTypeKey, PaymentType) VALUES
(1, 'credit_card'), (2, 'boleto'), (3, 'voucher'), (4, 'debit_card'), (5, 'not_defined');
GO