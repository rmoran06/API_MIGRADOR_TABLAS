using System.Data;
using Microsoft.Data.SqlClient;
using Dapper;
using Npgsql;

namespace DataMigratorApi.Services
{
    public class MigrationService
    {
        private readonly string _sqlConn;
        private readonly string _pgConn;

        public MigrationService(IConfiguration config)
        {
            _sqlConn = config.GetConnectionString("SqlServer") ?? throw new InvalidOperationException("SqlServer connection string missing");
            _pgConn = config.GetConnectionString("Postgres") ?? throw new InvalidOperationException("Postgres connection string missing");
        }

        public async Task<IEnumerable<object>> ListSqlServerTables()
        {
            const string query = @"
                select TABLE_SCHEMA, TABLE_NAME
                from INFORMATION_SCHEMA.TABLES
                where TABLE_TYPE='BASE TABLE'
                order by TABLE_SCHEMA, TABLE_NAME;";
            await using var conn = new SqlConnection(_sqlConn);
            return await conn.QueryAsync(query);
        }

        public async Task<IEnumerable<object>> ListPostgresTables()
        {
            const string query = @"
                select table_schema, table_name
                from information_schema.tables
                where table_type='BASE TABLE'
                order by table_schema, table_name;";
            await using var conn = new NpgsqlConnection(_pgConn);
            return await conn.QueryAsync(query);
        }

        public async Task<object> MigrateInvProductos()
        {
            var map = new (string src, string dst)[]
            {
                ("ACEPTA_ALTERNAS","acepta_alternas"),
                ("CANT_FACTURACION","cant_facturacion"),
                ("CANT_X_PALLETE","cant_x_pallete"),
                ("CANT_X_UNIDAD","cant_x_unidad"),
                ("COD_AGENTE","cod_agente"),
                ("COD_CASAFABRICANTE","cod_casafabricante"),
                ("COD_GRUPO","cod_grupo"),
                ("COD_IMPUESTO","cod_impuesto"),
                ("COD_MARCA","cod_marca"),
                ("COD_PRODUCTO","cod_producto"),
                ("COD_TIPOPROD","cod_tipoprod"),
                ("COD_UNIDAD_PEDIDO","cod_unidad_pedido"),
                ("COD_UPC_PROPIO","cod_upc_propio"),
                ("CUENTA_CONTABLE","cuenta_contable"),
                ("DESCRIPCION","descripcion"),
                ("DESCUENTO","descuento"),
                ("FACTURA_CAJA","factura_caja"),
                ("FEC_VENCIMIENTO","fec_vencimiento"),
                ("FLAG_COMPRADO","flag_comprado"),
                ("FLAG_KIT","flag_kit"),
                ("FLAG_PRODUCIDO","flag_producido"),
                ("KEYWORDS","keywords"),
                ("NOMBRE","nombre"),
                ("NOMBRE_INGLES","nombre_ingles"),
                ("NUM_CONTRATO","num_contrato"),
                ("PCTGE_COM_SUP_DFLT","pctge_com_sup_dflt"),
                ("PESO_UNIDAD","peso_unidad"),
                ("PICTURE","picture"),
                ("PICTURE_PATH","picture_path"),
                ("PRECIO_DOLARES","precio_dolares"),
                ("PRECIO_FOB","precio_fob"),
                ("PRECIO_LISTA","precio_lista"),
                ("PRECIO_VENTA","precio_venta"),
                ("PROY2_ABR","proy2_abr"),
                ("PROY2_AGO","proy2_ago"),
                ("PROY2_DIC","proy2_dic"),
                ("PROY2_ENE","proy2_ene"),
                ("PROY2_FEB","proy2_feb"),
                ("PROY2_JUL","proy2_jul"),
                ("PROY2_JUN","proy2_jun"),
                ("PROY2_MAR","proy2_mar"),
                ("PROY2_MAY","proy2_may"),
                ("PROY2_NOV","proy2_nov"),
                ("PROY2_OCT","proy2_oct"),
                ("PROY2_SEP","proy2_sep"),
                ("PROY_ABR","proy_abr"),
                ("PROY_ABR2","proy_abr2"),
                ("PROY_AGO","proy_ago"),
                ("PROY_AGO2","proy_ago2"),
                ("PROY_DIC","proy_dic"),
                ("PROY_DIC2","proy_dic2"),
                ("PROY_ENE","proy_ene"),
                ("PROY_ENE2","proy_ene2"),
                ("PROY_FEB","proy_feb"),
                ("PROY_FEB2","proy_feb2"),
                ("PROY_JUL","proy_jul"),
                ("PROY_JUL2","proy_jul2"),
                ("PROY_JUN","proy_jun"),
                ("PROY_JUN2","proy_jun2"),
                ("PROY_MAR","proy_mar"),
                ("PROY_MAR2","proy_mar2"),
                ("PROY_MAY","proy_may"),
                ("PROY_MAY2","proy_may2"),
                ("PROY_NOV","proy_nov"),
                ("PROY_NOV2","proy_nov2"),
                ("PROY_OCT","proy_oct"),
                ("PROY_OCT2","proy_oct2"),
                ("PROY_SEP","proy_sep"),
                ("PROY_SEP2","proy_sep2"),
                ("REBAJA_INVENTARIO","rebaja_inventario"),
                ("REP_PEDIDO2","rep_pedido2"),
                ("STATUS","status"),
                ("TOLERANCIA_VTA","tolerancia_vta"),
                ("VOLUMEN_UNIDAD","volumen_unidad"),
                ("VOLUMEN_UNIDAD2","volumen_unidad2"),
                ("ROWID","rowid"),
                ("CONTROLSERIES","controlseries"),
                ("ENSAMBLE_AUTOMATICO","ensamble_automatico"),
                ("WEB","web"),
                ("preciominorista","preciominorista"),
                ("preciomayoreo","preciomayoreo"),
                ("precioace","precioace"),
                ("precioclientea","precioclientea"),
                ("precioclienteb","precioclienteb"),
                ("precioclientec","precioclientec"),
                ("utilidad1","utilidad1"),
                ("utilidad2","utilidad2"),
                ("utilidad3","utilidad3"),
                ("utilidad4","utilidad4"),
                ("utilidad5","utilidad5"),
                ("utilidad6","utilidad6"),
                ("observaciones","observaciones"),
                ("CALCULA","calcula"),
                ("FORMULA","formula"),
                ("CUENTA_ASOCIADA","cuenta_asociada"),
                ("CUENTA_ASOCIADA_INGRESO","cuenta_asociada_ingreso"),
                ("FLAG_CUENTA_INGRESO","flag_cuenta_ingreso"),
                ("FLAG_GENERA_PDAISV","flag_genera_pdaisv"),
                ("FLAG_REVISADO","flag_revisado"),
                ("FLAG_SERVICIO","flag_servicio"),
                ("FLAG_HERRAMIENTA","flag_herramienta"),
                ("COD_UNIDAD_SALIDA","cod_unidad_salida"),
                ("FLAG_BOTELLON","flag_botellon"),
                ("FLAT_DESPACHO","flat_despacho")
            };

            var srcCols = string.Join(", ", map.Select(m => m.src));
            var dstCols = string.Join(", ", map.Select(m => m.dst));
            var paramCols = string.Join(", ", map.Select(m => "@" + m.dst));

            var selectSql = $"select {srcCols} from MERENDON.dbo.INV_PRODUCTOS";
            var insertSql = $"insert into siad.inv_productos({dstCols}) values ({paramCols})";

            int readCount = 0;
            int written = 0;

            await using var sqlConn = new SqlConnection(_sqlConn);
            await using var pgConn = new NpgsqlConnection(_pgConn);
            await pgConn.OpenAsync();

            // Limpiar antes de insertar (opcional, aquí vaciamos la tabla destino)
            await pgConn.ExecuteAsync("truncate table siad.inv_productos;");

            await using var reader = await sqlConn.ExecuteReaderAsync(selectSql);
            while (await reader.ReadAsync())
            {
                readCount++;
                var parameters = new DynamicParameters();
                foreach (var m in map)
                {
                    var val = reader[m.src];
                    // Npgsql espera Guid como Guid, no como string
                    if (m.dst == "rowid" && val is Guid g) parameters.Add(m.dst, g);
                    else parameters.Add(m.dst, val == DBNull.Value ? null : val);
                }
                written += await pgConn.ExecuteAsync(insertSql, parameters);
            }

            return new { read = readCount, written };
        }

        public async Task<object> MigrateInvExistencias(bool truncate = false, bool createIfMissing = false, bool skipOnPk = true)
        {
            var map = new (string src, string dst)[]
            {
                ("CANTIDAD_STOCK","cantidad_stock"),
                ("COD_BODEGA","cod_bodega"),
                ("COD_PRODUCTO","cod_producto"),
                ("COSTO_ACTUAL","costo_actual"),
                ("COSTO_ANTERIOR","costo_anterior"),
                ("COSTO_MAS_ALTO","costo_mas_alto"),
                ("COSTO_ULTIMO","costo_ultimo"),
                ("EST_BALANCEPROM","est_balanceprom"),
                ("FECHA_CREACION","fecha_creacion"),
                ("FECHA_MODIFICACION","fecha_modificacion"),
                ("ROTACION_ANUAL","rotacion_anual"),
                ("SALDO_MONETARIO","saldo_monetario"),
                ("STOCK_MAXIMO","stock_maximo"),
                ("STOCK_MINIMO","stock_minimo"),
                ("USUARIO_CREO","usuario_creo"),
                ("USUARIO_MODIFICA","usuario_modifica"),
                ("ROWID","rowid"),
                ("STOCK_REQUISADO","stock_requisado")
            };

            var srcCols = string.Join(", ", map.Select(m => m.src));
            var dstCols = string.Join(", ", map.Select(m => m.dst));

            var selectSql = $"select {srcCols} from MERENDON.dbo.INV_EXISTENCIAS";

            await using var sqlConn = new SqlConnection(_sqlConn);
            // Verificar existencia en SQL Server
            const string checkSrcSql = @"
                select 1 from INFORMATION_SCHEMA.TABLES
                where TABLE_SCHEMA = @schema and TABLE_NAME = @table";

            var srcExists = await sqlConn.ExecuteScalarAsync<int?>(checkSrcSql, new { schema = "dbo", table = "INV_EXISTENCIAS" }) != null;
            if (!srcExists) throw new InvalidOperationException("Source table dbo.INV_EXISTENCIAS not found in SQL Server");

            await using var pgConn = new NpgsqlConnection(_pgConn);
            await pgConn.OpenAsync();

            var dstExists = await pgConn.ExecuteScalarAsync<int?>("select 1 from information_schema.tables where table_schema=@schema and table_name=@table", new { schema = "siad", table = "inv_existencias" }) != null;
            if (!dstExists)
            {
                if (!createIfMissing) throw new InvalidOperationException("Destination table siad.inv_existencias not found in Postgres. Use createIfMissing=true to create it.");

                // Obtener tipos desde SQL Server para las columnas del mapa
                var colsMeta = (await sqlConn.QueryAsync(@"
                    select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable
                    from INFORMATION_SCHEMA.COLUMNS
                    where TABLE_SCHEMA=@schema and TABLE_NAME=@table
                    order by ordinal_position", new { schema = "dbo", table = "INV_EXISTENCIAS" })).ToList();

                var colDefs = new List<string>();
                foreach (var m in map)
                {
                    var c = colsMeta.FirstOrDefault(x => ((string)x.column_name).Equals(m.src, StringComparison.OrdinalIgnoreCase));
                    if (c == null) throw new InvalidOperationException($"Source column {m.src} not found in dbo.INV_EXISTENCIAS");
                    string type = MapSqlServerTypeToPostgres((string)c.data_type, (int?)c.character_maximum_length, (int?)c.numeric_precision, (int?)c.numeric_scale);
                    string nullable = ((string)c.is_nullable).Equals("YES", StringComparison.OrdinalIgnoreCase) ? "NULL" : "NOT NULL";
                    // use lowercase unquoted dest names to avoid case-sensitivity issues
                    colDefs.Add($"{m.dst.ToLower()} {type} {nullable}");
                }

                var createSql = $"create schema if not exists siad; create table siad.inv_existencias ({string.Join(", ", colDefs)});";
                await pgConn.ExecuteAsync(createSql);
                dstExists = true;
            }

            if (truncate)
            {
                await pgConn.ExecuteAsync("truncate table siad.inv_existencias;");
            }

            int read = 0;
            int inserted = 0;
            int skipped = 0;

            await using var tx = await pgConn.BeginTransactionAsync();

            // Crear tabla temporal y copiar los datos allí
            var tmpName = "tmp_" + Guid.NewGuid().ToString("N");
            await pgConn.ExecuteAsync($"create temp table {tmpName} (like siad.inv_existencias including defaults excluding constraints);");

            var dstColsForCopy = string.Join(", ", map.Select(m => m.dst.ToLower()));

            using (var importer = pgConn.BeginBinaryImport($"COPY {tmpName} ({dstColsForCopy}) FROM STDIN (FORMAT BINARY)"))
            {
                await using var reader = await sqlConn.ExecuteReaderAsync(selectSql);
                while (await reader.ReadAsync())
                {
                    read++;
                    importer.StartRow();
                    foreach (var m in map)
                    {
                        var val = reader[m.src];
                        if (val == DBNull.Value) importer.WriteNull();
                        else importer.Write(val);
                    }
                }
                importer.Complete();
            }

            // Obtener columnas PK de la tabla destino
            var pkCols = (await pgConn.QueryAsync<string>(@"select a.attname
from pg_index i
join pg_attribute a on a.attrelid = i.indrelid and a.attnum = ANY(i.indkey)
where i.indrelid = @reg::regclass and i.indisprimary", new { reg = $"siad.inv_existencias" })).ToList();

            if (skipOnPk)
            {
                if (!pkCols.Any())
                {
                    await tx.RollbackAsync();
                    throw new InvalidOperationException($"Destination table siad.inv_existencias has no primary key; cannot skip duplicates by PK. Add a primary key or set skipOnPk=false.");
                }

                var colsList = string.Join(", ", map.Select(c => $"\"{c.dst.ToLower()}\""));
                var pkList = string.Join(", ", pkCols.Select(c => $"\"{c}\""));
                var insertSql = $"insert into siad.inv_existencias ({colsList}) select {colsList} from {tmpName} ON CONFLICT ({pkList}) DO NOTHING;";
                inserted = await pgConn.ExecuteAsync(insertSql, transaction: tx);
                skipped = read - inserted;
            }
            else
            {
                var colsList = string.Join(", ", map.Select(c => $"\"{c.dst.ToLower()}\""));
                var insertSql = $"insert into siad.inv_existencias ({colsList}) select {colsList} from {tmpName};";
                inserted = await pgConn.ExecuteAsync(insertSql, transaction: tx);
                skipped = 0;
            }

            await tx.CommitAsync();

            return new { read, written = inserted, skipped };
        }

        public async Task<object> MigrateInvPrecioProducto()
        {
            var map = new (string src, string dst)[]
            {
                // No incluimos ID_LISTA para que Postgres genere su propia identidad
                ("COD_LISTA","cod_lista"),
                ("NOM_LISTA","nom_lista"),
                ("COD_PRODUCTO","cod_producto"),
                ("COD_TIPOCLIENTE","cod_tipocliente"),
                ("PRECIO","precio")
            };

            var srcCols = string.Join(", ", map.Select(m => m.src));
            var dstCols = string.Join(", ", map.Select(m => m.dst));
            var paramCols = string.Join(", ", map.Select(m => "@" + m.dst));

            var selectSql = $"select {srcCols} from MERENDON.dbo.INV_PRECIO_PRODUCTO";
            var insertSql = $"insert into siad.inv_precio_producto({dstCols}) values ({paramCols})";

            int readCount = 0;
            int written = 0;

            await using var sqlConn = new SqlConnection(_sqlConn);
            await using var pgConn = new NpgsqlConnection(_pgConn);
            await pgConn.OpenAsync();

            // Limpiar antes de insertar (opcional)
            await pgConn.ExecuteAsync("truncate table siad.inv_precio_producto;");

            await using var reader = await sqlConn.ExecuteReaderAsync(selectSql);
            while (await reader.ReadAsync())
            {
                readCount++;
                var parameters = new DynamicParameters();
                foreach (var m in map)
                {
                    var val = reader[m.src];
                    parameters.Add(m.dst, val == DBNull.Value ? null : val);
                }
                written += await pgConn.ExecuteAsync(insertSql, parameters);
            }

            return new { read = readCount, written };
        }

        public async Task<object> MigrateClnClientes(bool truncate = false, bool createIfMissing = false, bool skipOnPk = true)
        {
            const string srcSchema = "dbo";
            const string srcTable = "CLN_CLIENTES";
            const string dstSchema = "siad";
            const string dstTable = "cln_clientes";

            // Verificar existencia en SQL Server
            const string checkSrcSql = @"
                select 1 from INFORMATION_SCHEMA.TABLES
                where TABLE_SCHEMA = @schema and TABLE_NAME = @table";

            await using var sqlConn = new SqlConnection(_sqlConn);
            var srcExists = await sqlConn.ExecuteScalarAsync<int?>(checkSrcSql, new { schema = srcSchema, table = srcTable }) != null;
            if (!srcExists) throw new InvalidOperationException($"Source table {srcSchema}.{srcTable} not found in SQL Server");

            await using var pgConn = new NpgsqlConnection(_pgConn);
            await pgConn.OpenAsync();

            var dstExists = await pgConn.ExecuteScalarAsync<int?>("select 1 from information_schema.tables where table_schema=@schema and table_name=@table", new { schema = dstSchema, table = dstTable }) != null;
            if (!dstExists)
            {
                if (!createIfMissing) throw new InvalidOperationException($"Destination table {dstSchema}.{dstTable} not found in Postgres. Use createIfMissing=true to create it.");

                // Obtener columnas desde SQL Server
                var cols = (await sqlConn.QueryAsync(@"
                    select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable
                    from INFORMATION_SCHEMA.COLUMNS
                    where TABLE_SCHEMA=@schema and TABLE_NAME=@table
                    order by ordinal_position", new { schema = srcSchema, table = srcTable })).ToList();

                var colDefs = new List<string>();
                foreach (var c in cols)
                {
                    string name = c.column_name;
                    string type = MapSqlServerTypeToPostgres((string)c.data_type, (int?)c.character_maximum_length, (int?)c.numeric_precision, (int?)c.numeric_scale);
                    string nullable = ((string)c.is_nullable).Equals("YES", StringComparison.OrdinalIgnoreCase) ? "NULL" : "NOT NULL";
                    colDefs.Add($"\"{name}\" {type} {nullable}");
                }

                var createSql = $"create schema if not exists {dstSchema}; create table {dstSchema}.{dstTable} ({string.Join(", ", colDefs)});";
                await pgConn.ExecuteAsync(createSql);
                dstExists = true;
            }

            // Obtener lista de columnas para copiar
            var colNames = (await sqlConn.QueryAsync<string>(@"select column_name from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA=@schema and TABLE_NAME=@table order by ordinal_position", new { schema = srcSchema, table = srcTable })).ToList();
            if (!colNames.Any()) throw new InvalidOperationException("No columns found in source table");

            var selectSql = $"select {string.Join(", ", colNames)} from MERENDON.dbo.{srcTable}";

            if (truncate)
            {
                await pgConn.ExecuteAsync($"truncate table {dstSchema}.{dstTable};");
            }

            int read = 0;
            int inserted = 0;
            int skipped = 0;

            await using var tx = await pgConn.BeginTransactionAsync();

            // Crear tabla temporal (sin constraints) y copiar los datos allí
            var tmpName = "tmp_" + Guid.NewGuid().ToString("N");
            await pgConn.ExecuteAsync($"create temp table {tmpName} (like {dstSchema}.{dstTable} including defaults excluding constraints);");

            var dstColsForCopy = string.Join(", ", colNames.Select(c => c.ToLower()));

            using (var importer = pgConn.BeginBinaryImport($"COPY {tmpName} ({dstColsForCopy}) FROM STDIN (FORMAT BINARY)"))
            {
                await using var reader = await sqlConn.ExecuteReaderAsync(selectSql);
                while (await reader.ReadAsync())
                {
                    read++;
                    importer.StartRow();
                    foreach (var col in colNames)
                    {
                        var val = reader[col];
                        if (val == DBNull.Value) importer.WriteNull();
                        else importer.Write(val);
                    }
                }
                importer.Complete();
            }

            // Obtener columnas PK de la tabla destino
            var pkCols = (await pgConn.QueryAsync<string>(@"select a.attname
from pg_index i
join pg_attribute a on a.attrelid = i.indrelid and a.attnum = ANY(i.indkey)
where i.indrelid = @reg::regclass and i.indisprimary", new { reg = $"{dstSchema}.{dstTable}" })).ToList();

            if (skipOnPk)
            {
                if (!pkCols.Any())
                {
                    await tx.RollbackAsync();
                    throw new InvalidOperationException($"Destination table {dstSchema}.{dstTable} has no primary key; cannot skip duplicates by PK. Add a primary key or set skipOnPk=false.");
                }

                var colsList = string.Join(", ", colNames.Select(c => $"\"{c.ToLower()}\""));
                var pkList = string.Join(", ", pkCols.Select(c => $"\"{c}\""));
                var insertSql = $"insert into {dstSchema}.{dstTable} ({colsList}) select {colsList} from {tmpName} ON CONFLICT ({pkList}) DO NOTHING;";
                inserted = await pgConn.ExecuteAsync(insertSql, transaction: tx);
                skipped = read - inserted;
            }
            else
            {
                var colsList = string.Join(", ", colNames.Select(c => $"\"{c.ToLower()}\""));
                var insertSql = $"insert into {dstSchema}.{dstTable} ({colsList}) select {colsList} from {tmpName};";
                inserted = await pgConn.ExecuteAsync(insertSql, transaction: tx);
                skipped = 0;
            }

            await tx.CommitAsync();

            return new { read, written = inserted, skipped };
        }

        public async Task<object> MigrateTmpSeleccionCln(bool truncate = false, bool createIfMissing = false, bool skipOnPk = true)
        {
            const string srcSchema = "dbo";
            const string srcTable = "TMP_SELECCION_CLN";
            const string dstSchema = "siad";
            const string dstTable = "tmp_seleccion_cln";

            // Verificar existencia en SQL Server
            const string checkSrcSql = @"
                select 1 from INFORMATION_SCHEMA.TABLES
                where TABLE_SCHEMA = @schema and TABLE_NAME = @table";

            await using var sqlConn = new SqlConnection(_sqlConn);
            var srcExists = await sqlConn.ExecuteScalarAsync<int?>(checkSrcSql, new { schema = srcSchema, table = srcTable }) != null;
            if (!srcExists) throw new InvalidOperationException($"Source table {srcSchema}.{srcTable} not found in SQL Server");

            await using var pgConn = new NpgsqlConnection(_pgConn);
            await pgConn.OpenAsync();

            var dstExists = await pgConn.ExecuteScalarAsync<int?>("select 1 from information_schema.tables where table_schema=@schema and table_name=@table", new { schema = dstSchema, table = dstTable }) != null;
            if (!dstExists)
            {
                if (!createIfMissing) throw new InvalidOperationException($"Destination table {dstSchema}.{dstTable} not found in Postgres. Use createIfMissing=true to create it.");

                // Obtener columnas desde SQL Server
                var cols = (await sqlConn.QueryAsync(@"
                    select column_name, data_type, character_maximum_length, numeric_precision, numeric_scale, is_nullable
                    from INFORMATION_SCHEMA.COLUMNS
                    where TABLE_SCHEMA=@schema and TABLE_NAME=@table
                    order by ordinal_position", new { schema = srcSchema, table = srcTable })).ToList();

                var colDefs = new List<string>();
                foreach (var c in cols)
                {
                    string name = c.column_name;
                    string type = MapSqlServerTypeToPostgres((string)c.data_type, (int?)c.character_maximum_length, (int?)c.numeric_precision, (int?)c.numeric_scale);
                    string nullable = ((string)c.is_nullable).Equals("YES", StringComparison.OrdinalIgnoreCase) ? "NULL" : "NOT NULL";
                    colDefs.Add($"\"{name}\" {type} {nullable}");
                }

                var createSql = $"create schema if not exists {dstSchema}; create table {dstSchema}.{dstTable} ({string.Join(", ", colDefs)});";
                await pgConn.ExecuteAsync(createSql);
                dstExists = true;
            }

            // Obtener lista de columnas para copiar
            var colNames = (await sqlConn.QueryAsync<string>(@"select column_name from INFORMATION_SCHEMA.COLUMNS where TABLE_SCHEMA=@schema and TABLE_NAME=@table order by ordinal_position", new { schema = srcSchema, table = srcTable })).ToList();
            if (!colNames.Any()) throw new InvalidOperationException("No columns found in source table");

            var selectSql = $"select {string.Join(", ", colNames)} from MERENDON.dbo.{srcTable}";

            if (truncate)
            {
                await pgConn.ExecuteAsync($"truncate table {dstSchema}.{dstTable};");
            }

            int read = 0;
            int inserted = 0;
            int skipped = 0;

            await using var tx = await pgConn.BeginTransactionAsync();

            // Crear tabla temporal (sin constraints) y copiar los datos allí
            var tmpName = "tmp_" + Guid.NewGuid().ToString("N");
            await pgConn.ExecuteAsync($"create temp table {tmpName} (like {dstSchema}.{dstTable} including defaults excluding constraints);");

            var dstColsForCopy = string.Join(", ", colNames.Select(c => c.ToLower()));

            using (var importer = pgConn.BeginBinaryImport($"COPY {tmpName} ({dstColsForCopy}) FROM STDIN (FORMAT BINARY)"))
            {
                await using var reader = await sqlConn.ExecuteReaderAsync(selectSql);
                while (await reader.ReadAsync())
                {
                    read++;
                    importer.StartRow();
                    foreach (var col in colNames)
                    {
                        var val = reader[col];
                        if (val == DBNull.Value) importer.WriteNull();
                        else importer.Write(val);
                    }
                }
                importer.Complete();
            }

            // Obtener columnas PK de la tabla destino
            var pkCols = (await pgConn.QueryAsync<string>(@"select a.attname
from pg_index i
join pg_attribute a on a.attrelid = i.indrelid and a.attnum = ANY(i.indkey)
where i.indrelid = @reg::regclass and i.indisprimary", new { reg = $"{dstSchema}.{dstTable}" })).ToList();

            if (skipOnPk)
            {
                if (!pkCols.Any())
                {
                    await tx.RollbackAsync();
                    throw new InvalidOperationException($"Destination table {dstSchema}.{dstTable} has no primary key; cannot skip duplicates by PK. Add a primary key or set skipOnPk=false.");
                }

                var colsList = string.Join(", ", colNames.Select(c => $"\"{c.ToLower()}\""));
                var pkList = string.Join(", ", pkCols.Select(c => $"\"{c}\""));
                var insertSql = $"insert into {dstSchema}.{dstTable} ({colsList}) select {colsList} from {tmpName} ON CONFLICT ({pkList}) DO NOTHING;";
                inserted = await pgConn.ExecuteAsync(insertSql, transaction: tx);
                skipped = read - inserted;
            }
            else
            {
                var colsList = string.Join(", ", colNames.Select(c => $"\"{c.ToLower()}\""));
                var insertSql = $"insert into {dstSchema}.{dstTable} ({colsList}) select {colsList} from {tmpName};";
                inserted = await pgConn.ExecuteAsync(insertSql, transaction: tx);
                skipped = 0;
            }

            await tx.CommitAsync();

            return new { read, written = inserted, skipped };
        }

        private static string MapSqlServerTypeToPostgres(string sqlType, int? charMaxLen, int? prec, int? scale)
        {
            sqlType = sqlType?.ToLowerInvariant() ?? "";
            return sqlType switch
            {
                "uniqueidentifier" => "uuid",
                "nvarchar" => charMaxLen.HasValue && charMaxLen > 0 ? $"varchar({charMaxLen})" : "text",
                "varchar" => charMaxLen.HasValue && charMaxLen > 0 ? $"varchar({charMaxLen})" : "text",
                "text" => "text",
                "ntext" => "text",
                "int" => "integer",
                "bigint" => "bigint",
                "smallint" => "smallint",
                "tinyint" => "smallint",
                "bit" => "boolean",
                "decimal" => (prec.HasValue && scale.HasValue) ? $"numeric({prec},{scale})" : "numeric",
                "numeric" => (prec.HasValue && scale.HasValue) ? $"numeric({prec},{scale})" : "numeric",
                "money" => "numeric",
                "smallmoney" => "numeric",
                "float" => "double precision",
                "real" => "real",
                "date" => "date",
                "datetime" => "timestamp",
                "datetime2" => "timestamp",
                "smalldatetime" => "timestamp",
                "time" => "time",
                "binary" => "bytea",
                "varbinary" => "bytea",
                _ => "text"
            };
        }
    }
}
