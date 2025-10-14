using Es.Riam.Gnoss.Elementos.Suscripcion;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.Win.ServicioLiveUsuarios;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Gnoss.BackgroundTask.CommunityWall
{
    public class CommunityWallWorker : Worker
    {
        private readonly ConfigService _configService;
        private ILogger mlogger;
        private ILoggerFactory mLoggerFactory;
        public CommunityWallWorker(ConfigService configService, IServiceScopeFactory scopeFactory, ILogger<CommunityWallWorker> logger, ILoggerFactory loggerFactory)
            : base(logger, scopeFactory)
        {
            _configService = configService;
            mlogger = logger;
            mLoggerFactory = loggerFactory;
        }

        protected override List<ControladorServicioGnoss> ObtenerControladores()
        {
            List<ControladorServicioGnoss> controladores = new List<ControladorServicioGnoss>();
            controladores.Add(new ControladorLiveUsuarios(ScopedFactory, _configService, mLoggerFactory.CreateLogger<ControladorLiveUsuarios>(), mLoggerFactory));
            return controladores;
        }
    }
}
