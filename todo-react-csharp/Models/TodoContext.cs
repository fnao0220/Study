using Microsoft.EntityFrameworkCore;
using System.Collections.Generic;
using todo_react_csharp.Models;

namespace todo_react_app.Models;
public class TodoContext : DbContext
{
    public TodoContext(DbContextOptions<TodoContext> options)
        : base(options)
    {
    }

    public DbSet<TodoItem> TodoItem { get; set; } = null!;
}
