IF OBJECT_ID('dbo.usp_GetSPScriptDependencies', 'P') IS NOT NULL
DROP PROCEDURE dbo.usp_GetSPScriptDependencies;
GO

CREATE PROCEDURE dbo.usp_GetSPScriptDependencies
    @SPName NVARCHAR(256) -- Nombre del Stored Procedure
AS
BEGIN
    SET NOCOUNT ON;
    SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED; -- Para evitar bloqueos al leer metadatos

    DECLARE @ObjectID INT;

    -- Validar que el SP exista
    SELECT @ObjectID = OBJECT_ID(@SPName);

    IF @ObjectID IS NULL
    BEGIN
        RAISERROR('El Stored Procedure "%s" no existe en la base de datos actual.', 16, 1, @SPName);
        RETURN;
    END

    PRINT '---------------------------------------------------';
    PRINT 'DEPENDENCIAS DEL SP: ' + @SPName;
    PRINT '---------------------------------------------------';

    -- 1. Objetos a los que este SP hace referencia (objetos "referenciados" por el SP)
    PRINT CHAR(13) + '--- OBJETOS REFERENCIADOS POR ESTE SP (usa: sys.sql_expression_dependencies) ---';
    SELECT
        OBJECT_NAME(sed.referencing_id) AS ReferencingObject,
        OBJECT_NAME(sed.referenced_id) AS ReferencedObject,
        sed.referenced_class_desc AS ReferencedObjectType,
        sed.referenced_entity_name AS ReferencedEntityName,
        sed.referenced_schema_name AS ReferencedSchemaName,
        sed.is_caller_dependent AS IsCallerDependent,
        sed.is_ambiguous AS IsAmbiguous
    FROM
        sys.sql_expression_dependencies AS sed
    WHERE
        sed.referencing_id = @ObjectID
    ORDER BY
        sed.referenced_class_desc, sed.referenced_entity_name;

    -- 2. Objetos que hacen referencia a este SP (objetos que usan este SP)
    PRINT CHAR(13) + '--- OBJETOS QUE HACEN REFERENCIA A ESTE SP (usa: sys.dm_sql_referencing_entities) ---';
    SELECT
        referencing_schema_name AS ReferencingSchema,
        referencing_entity_name AS ReferencingObject,
        referencing_class_desc AS ReferencingObjectType,
        is_caller_dependent AS IsCallerDependent
    FROM
        sys.dm_sql_referencing_entities(@SPName, 'OBJECT') -- Se debe pasar el nombre completo (schema.object)
    ORDER BY
        referencing_class_desc, referencing_entity_name;

    -- 3. Una alternativa para objetos referenciados, usando sys.dm_sql_referenced_entities
    -- Esta vista puede dar más detalles sobre columnas, parámetros, etc.
    PRINT CHAR(13) + '--- OBJETOS REFERENCIADOS POR ESTE SP (alternativa: sys.dm_sql_referenced_entities) ---';
    SELECT
        referenced_schema_name AS ReferencedSchema,
        referenced_entity_name AS ReferencedObject,
        referenced_minor_name AS ReferencedMinorName, -- Ej: nombre de columna o parámetro
        referenced_class_desc AS ReferencedClassType,
        is_ambiguous AS IsAmbiguous,
        is_selected AS IsSelected,
        is_updated AS IsUpdated,
        is_select_all AS IsSelectAll
    FROM
        sys.dm_sql_referenced_entities(@SPName, 'OBJECT')
    WHERE
        referenced_id IS NOT NULL -- Excluir entidades que no tienen ID (como variables locales)
    ORDER BY
        referenced_class_desc, referenced_entity_name;

    PRINT CHAR(13) + '---------------------------------------------------';
    PRINT 'FIN DE LAS DEPENDENCIAS';
    PRINT '---------------------------------------------------';

END;
GO
