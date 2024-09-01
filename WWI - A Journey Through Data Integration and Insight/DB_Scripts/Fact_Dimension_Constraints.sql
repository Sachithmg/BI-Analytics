USE [WideWorldImportersPurchase]
GO

--Fact.Movement
ALTER TABLE [Fact].[Movement]
ALTER COLUMN movement_key bigint NOT NULL;

ALTER TABLE [Fact].[Movement]
ADD CONSTRAINT PK_Movement PRIMARY KEY (movement_key);

-- Ensure the column is NOT NULL
ALTER TABLE [Dimension].[Stock Item]
ALTER COLUMN stock_item_key bigint NOT NULL;

-- Add a primary key constraint
ALTER TABLE [Dimension].[Stock Item]
ADD CONSTRAINT PK_StockItem PRIMARY KEY (stock_item_key);

ALTER TABLE [Fact].[Movement]
ADD CONSTRAINT FK_Movement_StockItem FOREIGN KEY (stock_item_key)
REFERENCES [Dimension].[Stock Item] (stock_item_key);

-- Modify the column to ensure it is NOT NULL
ALTER TABLE [Dimension].[Supplier]
ALTER COLUMN supplier_key bigint NOT NULL;

-- Add a primary key constraint
ALTER TABLE [Dimension].[Supplier]
ADD CONSTRAINT PK_Supplier PRIMARY KEY (supplier_key);

ALTER TABLE [Fact].[Movement]
ADD CONSTRAINT FK_Movement_Supplier FOREIGN KEY (supplier_key)
REFERENCES [Dimension].[Supplier] (supplier_key);

-- Alter the column to NOT NULL if necessary
ALTER TABLE [Dimension].[Transaction Type]
ALTER COLUMN transaction_type_key bigint NOT NULL;

-- Add a primary key constraint
ALTER TABLE [Dimension].[Transaction Type]
ADD CONSTRAINT PK_TransactionType PRIMARY KEY (transaction_type_key);


ALTER TABLE [Fact].[Movement]
ADD CONSTRAINT FK_Movement_TransactionType FOREIGN KEY (transaction_type_key)
REFERENCES [Dimension].[Transaction Type] (transaction_type_key);

ALTER TABLE [Fact].[Movement]
ADD CONSTRAINT FK_Movement_Date FOREIGN KEY (date_key)
REFERENCES [Dimension].[Date] ([Date]);

ALTER TABLE [Fact].[Movement]
ADD payment_method_key bigint;




--Fact.Stock Holding
ALTER TABLE [Fact].[Stock Holding]
ALTER COLUMN stock_holding_key bigint NOT NULL;


ALTER TABLE [Fact].[Stock Holding]
ADD CONSTRAINT PK_StockHolding PRIMARY KEY (stock_holding_key);

ALTER TABLE [Fact].[Stock Holding]
ADD CONSTRAINT FK_StockHolding_StockItem FOREIGN KEY (stock_item_key)
REFERENCES [Dimension].[Stock Item] (stock_item_key);


--Fact.Purchase
ALTER TABLE [Fact].[Purchase]
ALTER COLUMN purchase_key bigint NOT NULL;

ALTER TABLE [Fact].[Purchase]
ADD CONSTRAINT PK_Purchase PRIMARY KEY (purchase_key);

ALTER TABLE [Fact].[Purchase]
ADD CONSTRAINT FK_Purchase_StockItem FOREIGN KEY (stock_item_key)
REFERENCES [Dimension].[Stock Item] (stock_item_key);

ALTER TABLE [Fact].[Purchase]
ADD CONSTRAINT FK_Purchase_Date FOREIGN KEY (date_key)
REFERENCES [Dimension].[Date] ([Date]);

ALTER TABLE [Fact].[Purchase]
ADD CONSTRAINT FK_Purchase_Supplier FOREIGN KEY (supplier_key)
REFERENCES [Dimension].[Supplier] (supplier_key);


--Fact.PurchaseOrderSummary
ALTER TABLE [Fact].[PurchaseOrderSummary]
ALTER COLUMN purchase_order_key int NOT NULL;


ALTER TABLE [Fact].[PurchaseOrderSummary]
ADD CONSTRAINT PK_PurchaseOrderSummary PRIMARY KEY (purchase_order_key);

ALTER TABLE [Fact].[PurchaseOrderSummary]
ADD CONSTRAINT FK_PurchaseOrderSummary_StockItem FOREIGN KEY (stock_item_key)
REFERENCES [Dimension].[Stock Item] (stock_item_key);

ALTER TABLE [Fact].[PurchaseOrderSummary]
ADD CONSTRAINT FK_PurchaseOrderSummary_Date FOREIGN KEY (order_date_key)
REFERENCES [Dimension].[Date] ([Date]);

ALTER TABLE [Fact].[PurchaseOrderSummary]
ADD CONSTRAINT FK_PurchaseOrderSummary_Supplier FOREIGN KEY (supplier_key)
REFERENCES [Dimension].[Supplier] (supplier_key);


-- Fact.Transaction
ALTER TABLE [Fact].[Transaction]
ALTER COLUMN transaction_key bigint NOT NULL;


ALTER TABLE [Fact].[Transaction]
ADD CONSTRAINT PK_Transaction PRIMARY KEY (transaction_key);

ALTER TABLE [Fact].[Transaction]
ADD CONSTRAINT FK_Transaction_Date FOREIGN KEY ([Date])
REFERENCES [Dimension].[Date] ([Date]);

-- If no results return, it means you need to set it as a primary key
ALTER TABLE [Dimension].[Customer]
ALTER COLUMN customer_key bigint NOT NULL;

ALTER TABLE [Dimension].[Customer]
ADD CONSTRAINT PK_Customer PRIMARY KEY (customer_key);

ALTER TABLE [Fact].[Transaction]
ADD CONSTRAINT FK_Transaction_Customer FOREIGN KEY (customer_key)
REFERENCES [Dimension].[Customer] (customer_key);

ALTER TABLE [Fact].[Transaction]
ADD CONSTRAINT FK_Transaction_Supplier FOREIGN KEY (supplier_key)
REFERENCES [Dimension].[Supplier] (supplier_key);

ALTER TABLE [Fact].[Transaction]
ADD CONSTRAINT FK_Transaction_TransactionType FOREIGN KEY (transaction_type_key)
REFERENCES [Dimension].[Transaction Type] (transaction_type_key);

-- If no primary key exists, modify the column to NOT NULL and add a primary key
ALTER TABLE [Dimension].[Payment Method]
ALTER COLUMN Payment_Method_Key bigint NOT NULL;

ALTER TABLE [Dimension].[Payment Method]
ADD CONSTRAINT PK_PaymentMethod PRIMARY KEY (Payment_Method_Key);

ALTER TABLE [Fact].[Transaction]
ADD CONSTRAINT FK_Transaction_PaymentMethod FOREIGN KEY (payment_method_key)
REFERENCES [Dimension].[Payment Method] (Payment_Method_Key);
