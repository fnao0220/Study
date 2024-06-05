using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Primitives;

namespace MvcMovie.Controllers
{
    public class HelloWorldController : Controller
    {
        public string Index()
        {
            return "Hello sorld";
        }
        public string Welcome(string name, int age = 1)
        {
            return $"名前｣{name},{age}";
        }
    }
}
