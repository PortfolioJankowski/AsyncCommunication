using BuildingBlocks.Db;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using SomeBackgroundService;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddScoped<IDbRepository, DbRepository>();
builder.Services.Decorate<IDbRepository, CachedDbRepositoryDecorator>();
builder.Services.AddDbContext<ApiDbContext>(options =>
{
    options.UseSqlite(@"Data Source=C:\\Users\\matja\\source\\repos\\Duracell\\SomeBackgroundService\\database.db");
});
builder.Services.AddStackExchangeRedisCache(options =>
{
    options.Configuration = builder.Configuration.GetConnectionString("Redis");
});

builder.Services.AddHostedService<OutboxService>();
builder.Services.AddHostedService<OutboxPublisherService>();
var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var context = scope.ServiceProvider.GetRequiredService<ApiDbContext>();

    context.Database.Migrate();
}

app.UseHttpsRedirection();

app.MapGet("/", () =>
{
    return "Service is running.";
});

Thread.Sleep(5000); // Poczekaj na uruchomienie zale¿nych us³ug
app.Run();


