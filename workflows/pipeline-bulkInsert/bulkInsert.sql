BULK INSERT Cliente
	FROM 'C:\Users\Carlos\projects\projects-datadeveloper\workflows\pipeline-bulkInsert\Clientes.csv'
		WITH 
			(
	
			FORMAT = 'CSV',
			FIRSTROW = 2,
			FIELDTERMINATOR = ','
			
			)
