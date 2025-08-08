using Microsoft.AspNetCore.Mvc;

namespace EvoAPI.Api.Controllers
{
    [ApiController]
    [Route("api/[controller]")]
    public abstract class BaseController : ControllerBase
    {
        // Common functionality for all controllers can go here
    }
}
