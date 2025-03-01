SET NOCOUNT ON;

IF OBJECT_ID('tempdb..##tableTesting') IS NOT NULL
    DROP TABLE ##tableTesting;

CREATE TABLE ##tableTesting (
    idStuden		INT PRIMARY KEY NOT NULL,
    nameStuden		NVARCHAR(30) NOT NULL UNIQUE,
    ageStudent		TINYINT NOT NULL,
    sourceCourse	NVARCHAR(30)
);

DECLARE @contador AS INT SET @contador = 1;

WHILE @contador < 50
BEGIN
    INSERT INTO ##tableTesting
    VALUES(@contador, CONCAT('Estudiante_', @contador), 13+@contador, CONCAT('Curso_', @contador));

    SET @contador = @contador + 1;
END;

SELECT * FROM ##tableTesting;

