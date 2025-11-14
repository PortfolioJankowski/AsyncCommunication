using BuildingBlocks.Db;
using Microsoft.Extensions.DependencyInjection;
using SomeBackgroundService;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddScoped<IDbRepository, DbRepository>();
builder.Services.Decorate<IDbRepository, CachedDbRepositoryDecorator>();
builder.Services.AddDbContext<ApiDbContext>();

builder.Services.AddStackExchangeRedisCache(options =>
{
    options.Configuration = builder.Configuration.GetConnectionString("Redis");
});

builder.Services.AddHostedService<OutboxService>();
builder.Services.AddHostedService<FinalService>();

var app = builder.Build();
app.UseHttpsRedirection();

app.MapGet("/", () =>
{
    return "Service is running.";
});
app.Run();


