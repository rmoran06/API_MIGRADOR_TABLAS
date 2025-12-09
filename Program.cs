using DataMigratorApi.Services;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddCors(o => o.AddPolicy("AllowAll", p => p.AllowAnyOrigin().AllowAnyMethod().AllowAnyHeader()));
builder.Services.AddSingleton<MigrationService>();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();

var app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.UseSwagger();
    app.UseSwaggerUI();
}

app.UseCors("AllowAll");

// Lista tablas (SQL Server)
app.MapGet("/api/sqlserver/tables", async (MigrationService svc) =>
{
    var tables = await svc.ListSqlServerTables();
    return Results.Ok(tables);
});

// Lista tablas (Postgres)
app.MapGet("/api/postgres/tables", async (MigrationService svc) =>
{
    var tables = await svc.ListPostgresTables();
    return Results.Ok(tables);
});

// Migra INV_PRODUCTOS -> inv_productos
app.MapPost("/api/migrate/inv_productos", async (MigrationService svc) =>
{
    var result = await svc.MigrateInvProductos();
    return Results.Ok(result);
});

// Migra INV_EXISTENCIAS -> inv_existencias
app.MapPost("/api/migrate/inv_existencias", async (MigrationService svc) =>
{
    var result = await svc.MigrateInvExistencias();
    return Results.Ok(result);
});

// Migra INV_PRECIO_PRODUCTO -> inv_precio_producto
app.MapPost("/api/migrate/inv_precio_producto", async (MigrationService svc) =>
{
    var result = await svc.MigrateInvPrecioProducto();
    return Results.Ok(result);
});

app.Run();
