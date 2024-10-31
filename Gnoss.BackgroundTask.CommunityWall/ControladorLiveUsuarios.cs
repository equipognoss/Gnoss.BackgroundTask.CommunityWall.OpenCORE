using System;
using System.Collections.Generic;
using System.Threading;
using System.IO;
using System.Reflection;
using Es.Riam.Util;
using Es.Riam.Gnoss.Util.General;
using Es.Riam.Gnoss.Logica.Live;
using Es.Riam.Gnoss.AD.Live.Model;
using Es.Riam.Gnoss.AD.Live;
using Es.Riam.Gnoss.Logica.Identidad;
using Es.Riam.Gnoss.Logica.Usuarios;
using Es.Riam.Gnoss.CL.Live;
using Es.Riam.Gnoss.Logica.ParametroAplicacion;
using Es.Riam.Gnoss.Recursos;
using Es.Riam.Gnoss.Logica.Documentacion;
using Es.Riam.Gnoss.Logica.ServiciosGenerales;
using Es.Riam.Gnoss.AD.ServiciosGenerales;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.CL.Documentacion;
using Es.Riam.Gnoss.CL;
using Es.Riam.Gnoss.CL.ServiciosGenerales;
using Es.Riam.Gnoss.AD.Usuarios;
using Es.Riam.Gnoss.Web.Controles.ParametroAplicacionGBD;
using Es.Riam.Gnoss.Elementos.ParametroAplicacion;
using Es.Riam.Gnoss.AD.EncapsuladoDatos;
using Es.Riam.Gnoss.RabbitMQ;
using Newtonsoft.Json;
using Es.Riam.Gnoss.AD.Documentacion;
using Es.Riam.Gnoss.AD.EntityModel.Models.ProyectoDS;
using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.AD.EntityModel;
using Es.Riam.Gnoss.AD.EntityModelBASE;
using Es.Riam.Gnoss.AD.Virtuoso;
using Es.Riam.Gnoss.AD.BASE_BD;
using Es.Riam.AbstractsOpen;

namespace Es.Riam.Gnoss.Win.ServicioLiveUsuarios
{
    internal class ControladorLiveUsuarios : ControladorServicioGnoss
    {
        #region

        private const string COLA_USUARIOS = "ColaUsuarios";
        private const string EXCHANGE = "";

        #endregion

        #region Miembros

        /// <summary>
        /// Almacena el último Score que se ha asginado a cada perfil de usuario
        /// </summary>
        private Dictionary<Guid, int> mListaScorePorPerfil = new Dictionary<Guid, int>();

        private Dictionary<string, int> mListaScorePorProyUsuSuscr = new Dictionary<string, int>();

        private Dictionary<string, int> mListaScorePorUsuSuscr = new Dictionary<string, int>();

        private Dictionary<string, int> mListaScorePorProyUsu = new Dictionary<string, int>();

        private Dictionary<string, int> mListaScorePorProyGru = new Dictionary<string, int>();

        private Dictionary<string, int> mListaScorePorProyPerfilUsu = new Dictionary<string, int>();

        private Dictionary<string, int> mListaScorePorProyPerfilOrg = new Dictionary<string, int>();

        private Guid mElementoID;

        private List<Guid> mListaAutoresID = new List<Guid>();

        private List<Guid> mListaOrganizacionesIDAutores = new List<Guid>();

        #endregion

        #region Constructores

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="pFicheroConfiguracionSitioWeb">Ruta al archivo de configuración del sitio Web</param>
        public ControladorLiveUsuarios(IServiceScopeFactory scopedFactory, ConfigService configService)
            : base(scopedFactory, configService)
        {
        }

        protected override ControladorServicioGnoss ClonarControlador()
        {
            return new ControladorLiveUsuarios(ScopedFactory, mConfigService);
        }

        #endregion

        #region Métodos generales

        private void EstablecerDominioCache(EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            bool correcto = false;
            while (!correcto)
            {
                try
                {
                    ParametroAplicacionCN parametroApliCN = new ParametroAplicacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                    //ParametroAplicacionDS paramApliDS = parametroApliCN.ObtenerConfiguracionGnoss();
                    //parametroApliCN.Dispose();
                    ParametroAplicacionGBD parametroAplicionGBD = new ParametroAplicacionGBD(loggingService, entityContext, mConfigService);
                    GestorParametroAplicacion gestorParametroAplicacion = new GestorParametroAplicacion();
                    parametroAplicionGBD.ObtenerConfiguracionGnoss(gestorParametroAplicacion);
                    //mDominio = paramApliDS.ParametroAplicacion.Select("Parametro='UrlIntragnoss'").Valor;
                    mDominio = gestorParametroAplicacion.ParametroAplicacion.Find(parametroAPlicacion=>parametroAPlicacion.Parametro.Equals("UrlIntragnoss")).Valor;
                    mDominio = mDominio.Replace("http://", "").Replace("www.", "");

                    if (mDominio[mDominio.Length - 1] == '/')
                    {
                        mDominio = mDominio.Substring(0, mDominio.Length - 1);
                    }
                    correcto = true;
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLog(loggingService.DevolverCadenaError(ex, "1.0"));
                    Thread.Sleep(1000);
                }
            }
        }

        private void ProcesarFilaColaUsuarios(LiveUsuariosDS.ColaUsuariosRow pColaUsuario,EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            ComprobarCancelacionHilo();
            try
            {
                ProcesarFila(pColaUsuario, entityContext, loggingService, redisCacheWrapper, virtuosoAD, servicesUtilVirtuosoAndReplication);
                pColaUsuario.NumIntentos = 7;
            }
            catch (Exception ex)
            {
                loggingService.GuardarLogError(ex);
            }
        }
        
        private bool ProcesarItem(string pFila)
        {
            using (var scope = ScopedFactory.CreateScope())
            {
                EntityContext entityContext = scope.ServiceProvider.GetRequiredService<EntityContext>();
                entityContext.SetTrackingFalse();
                RedisCacheWrapper redisCacheWrapper = scope.ServiceProvider.GetRequiredService<RedisCacheWrapper>();
                LoggingService loggingService = scope.ServiceProvider.GetRequiredService<LoggingService>();
                VirtuosoAD virtuosoAD = scope.ServiceProvider.GetRequiredService<VirtuosoAD>();
                ConfigService configService = scope.ServiceProvider.GetRequiredService<ConfigService>();
                IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication = scope.ServiceProvider.GetRequiredService<IServicesUtilVirtuosoAndReplication>();
                ComprobarTraza("CommunityWall", entityContext, loggingService, redisCacheWrapper, configService, servicesUtilVirtuosoAndReplication);
                try
                {
                    ComprobarCancelacionHilo();

                    System.Diagnostics.Debug.WriteLine($"ProcesarItem, {pFila}!");

                    if (!string.IsNullOrEmpty(pFila))
                    {
                        object[] itemArray = JsonConvert.DeserializeObject<object[]>(pFila);
                        LiveUsuariosDS.ColaUsuariosRow filaColaUsuario = (LiveUsuariosDS.ColaUsuariosRow)new LiveUsuariosDS().ColaUsuarios.Rows.Add(itemArray);
                        itemArray = null;

                        ProcesarFilaColaUsuarios(filaColaUsuario,entityContext, loggingService, redisCacheWrapper, virtuosoAD, servicesUtilVirtuosoAndReplication);

                        filaColaUsuario = null;

                        ControladorConexiones.CerrarConexiones(false);
                    }
                    return true;
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLogError(ex);
                    return true;
                }
                finally
                {
                    GuardarTraza(loggingService);
                }
            }
        }

        public void RealizarMantenimientoRabbitMQ(LoggingService loggingService, bool reintentar = true)
        {
            if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
            {
                RabbitMQClient.ReceivedDelegate funcionProcesarItem = new RabbitMQClient.ReceivedDelegate(ProcesarItem);
                RabbitMQClient.ShutDownDelegate funcionShutDown = new RabbitMQClient.ShutDownDelegate(OnShutDown);

                RabbitMQClient rabbitMQClient = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, "ColaUsuarios",loggingService, mConfigService, "", "ColaUsuarios");

                try
                {
                    rabbitMQClient.ObtenerElementosDeCola(funcionProcesarItem, funcionShutDown);
                    mReiniciarLecturaRabbit = false;
                }
                catch (Exception ex)
                {
                    mReiniciarLecturaRabbit = true;
                    loggingService.GuardarLogError(ex);
                }
            }
        }

        public override void RealizarMantenimiento(EntityContext entityContext, EntityContextBASE entityContextBASE, UtilidadesVirtuoso utilidadesVirtuoso, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            try
            {
                //UtilTrazas.TrazaHabilitada = true;

                EstablecerDominioCache(entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                BaseCL.mLanzarExcepciones = true;
                BaseCL.mUsarHilos = false;

                RealizarMantenimientoRabbitMQ(loggingService);
                //RealizarMantenimientoBD();
                
            }
            catch (Exception ex)
            {
                loggingService.GuardarLog(loggingService.DevolverCadenaError(ex, "1.0"));
            }
        }

        /// <summary>
        /// Actualizamos la actividad reciente de la home del proyecto
        /// </summary>
        private void ActualizarLiveProyectoUsuario(LiveUsuariosDS.ColaUsuariosRow pFilaCola, Guid? pUsuarioID, LiveUsuariosCL pLiveUsuariosCL, string pNombreCacheElemento, bool pRecursoVisibleSoloParaMiembros)
        {
            if (pFilaCola.ProyectoId != ProyectoAD.MetaProyecto)
            {
                string claveProyUsu = pFilaCola.ProyectoId.ToString();
                if (pUsuarioID.HasValue && pUsuarioID.Value != Guid.Empty)
                {
                    claveProyUsu += pUsuarioID.Value;
                }

                AccionLive accion = (AccionLive)pFilaCola.Accion;

                if (!accion.Equals(AccionLive.Eliminado))
                {
                    int num = pLiveUsuariosCL.ObtenerNumElementosProyectoUsuario(pUsuarioID, pFilaCola.ProyectoId);

                    if (pUsuarioID.HasValue && pUsuarioID.Value != Guid.Empty && num == 0)
                    {
                        num = pLiveUsuariosCL.ClonarLiveProyectoAUsu(pUsuarioID.Value, pFilaCola.ProyectoId);
                    }

                    mListaScorePorProyUsu[claveProyUsu] = pLiveUsuariosCL.AgregarLiveProyectoUsuario(pUsuarioID, pFilaCola.ProyectoId, pNombreCacheElemento, ObtenerUltimoScoreProyectoUsuario(pFilaCola.ProyectoId, pUsuarioID));

                    if(!pUsuarioID.HasValue){
                        if (!pRecursoVisibleSoloParaMiembros)
                        {
                            string claveProyInvitado = pFilaCola.ProyectoId.ToString() + UsuarioAD.Invitado.ToString();

                            // Si el recurso es visible por todos y el usuario no tiene valor, se intenta agregar a la caché de usuario invitado.
                            mListaScorePorProyUsu[claveProyInvitado] = pLiveUsuariosCL.AgregarLiveProyectoUsuarioInvitado(pFilaCola.ProyectoId, pNombreCacheElemento, ObtenerUltimoScoreProyectoUsuario(pFilaCola.ProyectoId, UsuarioAD.Invitado));
                        }
                        else if (pRecursoVisibleSoloParaMiembros)
                        {
                            // Si el recurso no es visible por todos, se tiene que eliminar de la caché de usuario invitado.
                            pLiveUsuariosCL.EliminarLiveProyectoUsuarioInvitado(pFilaCola.ProyectoId, pNombreCacheElemento);
                        }
                    }

                    num++;

                    //cuantos elementos hay? > 100 eliminamos
                    if (num > 100)
                    {
                        int fin = num - 100;
                        pLiveUsuariosCL.EliminaElementosProyectoUsuario(pUsuarioID, pFilaCola.ProyectoId, 0, fin - 1);
                    }
                }
                else
                {
                    pLiveUsuariosCL.EliminarLiveProyectoUsuario(pUsuarioID, pFilaCola.ProyectoId, pNombreCacheElemento);

                    if (!pRecursoVisibleSoloParaMiembros && !pUsuarioID.HasValue)
                    {
                        // Si el recurso es visible por todos y se edita o cambia la privacidad, deberá borrarse para los invitados.
                        pLiveUsuariosCL.EliminarLiveProyectoUsuarioInvitado(pFilaCola.ProyectoId, pNombreCacheElemento);
                    }
                }
            }
        }

        /// <summary>
        /// Actualizamos la actividad reciente de la home del proyecto
        /// </summary>
        private void ActualizarLiveProyectoGrupo(LiveUsuariosDS.ColaUsuariosRow pFilaCola, Guid? pGrupoID, LiveUsuariosCL pLiveUsuariosCL, string pNombreCacheElemento)
        {
            if (pFilaCola.ProyectoId != ProyectoAD.MetaProyecto)
            {
                string claveProyGrupo = pFilaCola.ProyectoId.ToString();
                if (pGrupoID.HasValue)
                {
                    claveProyGrupo += pGrupoID.Value;
                }

                AccionLive accion = (AccionLive)pFilaCola.Accion;

                if (!accion.Equals(AccionLive.Eliminado))
                {
                    int num = pLiveUsuariosCL.ObtenerNumElementosProyectoGrupo(pGrupoID, pFilaCola.ProyectoId);

                    //if (pGrupoID.HasValue && num == 0)
                    //{
                    //    num = pLiveUsuariosCL.ClonarLiveProyectoAUsu(pGrupoID.Value, pFilaCola.ProyectoId);
                    //}

                    mListaScorePorProyGru[claveProyGrupo] = pLiveUsuariosCL.AgregarLiveProyectoGrupo(pGrupoID, pFilaCola.ProyectoId, pNombreCacheElemento, ObtenerUltimoScoreProyectoGrupo(pFilaCola.ProyectoId, pGrupoID));

                    num++;

                    //cuantos elementos hay? > 100 eliminamos
                    if (num > 100)
                    {
                        int fin = num - 100;
                        pLiveUsuariosCL.EliminaElementosProyectoGrupo(pGrupoID, pFilaCola.ProyectoId, 0, fin - 1);
                    }
                }
                else
                {
                    pLiveUsuariosCL.EliminarLiveProyectoGrupo(pGrupoID, pFilaCola.ProyectoId, pNombreCacheElemento);
                }
            }
        }

        /// <summary>
        /// Actualizamos la actividad reciente del perfil del usuario
        /// </summary>
        private void ActualizarLiveProyectoPerfilUsu(AccionLive pAccionLiveFilaCola, Guid pProyectoID, Guid pPerfilID, LiveUsuariosCL pLiveUsuariosCL, string pNombreCacheElemento)
        {
            if (pProyectoID != ProyectoAD.MetaProyecto)
            {
                if (!pAccionLiveFilaCola.Equals(AccionLive.Votado))
                {
                    string claveProyPerfilUsu = string.Concat(pProyectoID, pPerfilID);

                    if (!pAccionLiveFilaCola.Equals(AccionLive.Eliminado))
                    {
                        int num = pLiveUsuariosCL.ObtenerNumElementosProyectoPerfilUsuario(pPerfilID, pProyectoID);


                        mListaScorePorProyPerfilUsu[claveProyPerfilUsu] = pLiveUsuariosCL.AgregarLiveProyectoPerfilUsuario(pPerfilID, pProyectoID, pNombreCacheElemento, ObtenerUltimoScoreProyectoPerfilUsuario(pProyectoID, pPerfilID));

                        num++;

                        //cuantos elementos hay? > 100 eliminamos
                        if (num > 100)
                        {
                            int fin = num - 100;
                            pLiveUsuariosCL.EliminaElementosProyectoPerfilUsuario(pPerfilID, pProyectoID, 0, fin - 1);
                        }
                    }
                    else
                    {
                        pLiveUsuariosCL.EliminarLiveProyectoPerfilUsuario(pPerfilID, pProyectoID, pNombreCacheElemento);
                    }
                }
            }
        }

        /// <summary>
        /// Actualizamos la actividad reciente del perfil del usuario
        /// </summary>
        private void ActualizarLiveMetaProyectoPerfilUsu(AccionLive pAccionLiveFilaCola, Guid pPerfilID, LiveUsuariosCL pLiveUsuariosCL, string pNombreCacheElemento)
        {
            Guid metaProyectoID = ProyectoAD.MetaProyecto;

            if (!pAccionLiveFilaCola.Equals(AccionLive.Votado))
            {
                string claveProyPerfilUsu = string.Concat(metaProyectoID, pPerfilID);

                if (!pAccionLiveFilaCola.Equals(AccionLive.Eliminado))
                {
                    int num = pLiveUsuariosCL.ObtenerNumElementosProyectoPerfilUsuario(pPerfilID, metaProyectoID);

                    mListaScorePorProyPerfilUsu[claveProyPerfilUsu] = pLiveUsuariosCL.AgregarLiveProyectoPerfilUsuario(pPerfilID, metaProyectoID, pNombreCacheElemento, ObtenerUltimoScoreProyectoPerfilUsuario(metaProyectoID, pPerfilID));

                    num++;

                    //cuantos elementos hay? > 100 eliminamos
                    if (num > 100)
                    {
                        int fin = num - 100;
                        pLiveUsuariosCL.EliminaElementosProyectoPerfilUsuario(pPerfilID, metaProyectoID, 0, fin - 1);
                    }
                }
                else
                {
                    pLiveUsuariosCL.EliminarLiveProyectoPerfilUsuario(pPerfilID, metaProyectoID, pNombreCacheElemento);
                }
            }
        }

        /// <summary>
        /// Actualizamos la actividad reciente del perfil del usuario
        /// </summary>
        private void ActualizarLiveProyectoPerfilOrg(LiveUsuariosDS.ColaUsuariosRow pFilaCola, Guid pOrganizacionID, LiveUsuariosCL pLiveUsuariosCL, string pNombreCacheElemento)
        {
            if (pFilaCola.ProyectoId != ProyectoAD.MetaProyecto)
            {
                AccionLive accion = (AccionLive)pFilaCola.Accion;

                if (!accion.Equals(AccionLive.Votado))
                {
                    string claveProyPerfilOrg = string.Concat(pFilaCola.ProyectoId, pOrganizacionID);

                    if (!accion.Equals(AccionLive.Eliminado))
                    {
                        int num = pLiveUsuariosCL.ObtenerNumElementosProyectoPerfilOrg( pOrganizacionID, pFilaCola.ProyectoId);
                       
                        mListaScorePorProyPerfilOrg[claveProyPerfilOrg] = pLiveUsuariosCL.AgregarLiveProyectoPerfilOrg(pOrganizacionID, pFilaCola.ProyectoId, pNombreCacheElemento, ObtenerUltimoScoreProyectoPerfilOrg(pFilaCola.ProyectoId, pOrganizacionID));

                        num++;

                        //cuantos elementos hay? > 100 eliminamos
                        if (num > 100)
                        {
                            int fin = num - 100;
                            pLiveUsuariosCL.EliminaElementosProyectoPerfilOrg(pOrganizacionID, pFilaCola.ProyectoId, 0, fin - 1);
                        }
                    }
                    else
                    {
                        pLiveUsuariosCL.EliminarLiveProyectoPerfilOrg(pOrganizacionID, pFilaCola.ProyectoId, pNombreCacheElemento);
                    }
                }
            }
        }

        /// <summary>
        /// Actualizamos la actividad reciente del perfil del usuario
        /// </summary>
        private void ActualizarLiveMetaProyectoPerfilOrg(LiveUsuariosDS.ColaUsuariosRow pFilaCola, Guid pOrganizacionID, LiveUsuariosCL pLiveUsuariosCL, string pNombreCacheElemento)
        {
            Guid metaProyectoID = ProyectoAD.MetaProyecto;

            AccionLive accion = (AccionLive)pFilaCola.Accion;

            if (!accion.Equals(AccionLive.Votado))
            {
                string claveProyPerfilOrg = string.Concat(metaProyectoID, pOrganizacionID);

                if (!accion.Equals(AccionLive.Eliminado))
                {
                    int num = pLiveUsuariosCL.ObtenerNumElementosProyectoPerfilOrg(pOrganizacionID, metaProyectoID);

                    mListaScorePorProyPerfilOrg[claveProyPerfilOrg] = pLiveUsuariosCL.AgregarLiveProyectoPerfilOrg(pOrganizacionID, metaProyectoID, pNombreCacheElemento, ObtenerUltimoScoreProyectoPerfilOrg(metaProyectoID, pOrganizacionID));

                    num++;

                    //cuantos elementos hay? > 100 eliminamos
                    if (num > 100)
                    {
                        int fin = num - 100;
                        pLiveUsuariosCL.EliminaElementosProyectoPerfilOrg(pOrganizacionID, metaProyectoID, 0, fin - 1);
                    }
                }
                else
                {
                    pLiveUsuariosCL.EliminarLiveProyectoPerfilOrg(pOrganizacionID, metaProyectoID, pNombreCacheElemento);
                }
            }
        }

        private void ProcesarFila(LiveUsuariosDS.ColaUsuariosRow pFilaCola, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            LiveUsuariosCL liveUsuariosCL = new LiveUsuariosCL(entityContext, loggingService, redisCacheWrapper, mConfigService, servicesUtilVirtuosoAndReplication);
            liveUsuariosCL.Dominio = mDominio;

            ObtenerIDElementoPrincipal(pFilaCola, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

            mListaAutoresID.Clear();
            mListaOrganizacionesIDAutores.Clear();

            AccionLive accion = (AccionLive)pFilaCola.Accion;
            TipoLive tipo = (TipoLive)pFilaCola.Tipo;

            bool esDocumento = ((pFilaCola.Tipo == (int)TipoLive.Recurso) || (pFilaCola.Tipo == (int)TipoLive.Pregunta) || (pFilaCola.Tipo == (int)TipoLive.Debate));

            //Si la accion es sobre un comentario solamente insertamos la fila para acualizar el HTML
            bool accionSobreComentario = accion.Equals(AccionLive.ComentarioEliminado) || accion.Equals(AccionLive.ComentarioEditado);

            //Si la accion es editado y no se ha cambiado la privacidad solamente insertamos la fila para acualizar el HTML
            bool editarRecursoSinCambiarPrivacidad = accion.Equals(AccionLive.Editado) && !pFilaCola.InfoExtra.Contains(Constantes.PRIVACIDAD_CAMBIADA);

            // No hay que procesar los borradores.
            bool documentoBorrador = false;
            if (esDocumento)
            {
                DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                documentoBorrador = docCN.EsDocumentoBorrador(mElementoID);
                docCN.Dispose();
            }

            if (accionSobreComentario || editarRecursoSinCambiarPrivacidad || documentoBorrador)
            {
                return;
            }
            else if (esDocumento && !HayQueProcesarEvento(mElementoID, pFilaCola, entityContext, loggingService, servicesUtilVirtuosoAndReplication))
            {
                return;
            }

            string infoExtra = "";
            if (!pFilaCola.IsInfoExtraNull() && pFilaCola.InfoExtra != "" && pFilaCola.InfoExtra != "didactalia" && pFilaCola.InfoExtra != "didactalia" && !pFilaCola.InfoExtra.Contains(Constantes.PRIVACIDAD_CAMBIADA))
            {
                infoExtra = "_" + Constantes.PRIVACIDAD_CAMBIADA;
            }

            string nombreCacheElemento = pFilaCola.Tipo + "_" + mElementoID + "_" + pFilaCola.ProyectoId + infoExtra;

            bool recursoPrivado = false;
            if (esDocumento)
            {
                // Lista con los usuarios y los perfiles afectados por el evento
                Dictionary<Guid, List<Guid>> listaUsuariosAfectadosPorEvento = new Dictionary<Guid, List<Guid>>();

                // Lista con los grupos y sus perfiles afectados por el evento
                Dictionary<Guid, List<Guid>> listaGruposAfectadosPorEvento = new Dictionary<Guid, List<Guid>>();

                // Se obtiene la lista de usuarios
                ObtenerListaUsuariosDeProyectoAfectadosPorEvento(pFilaCola, ref listaUsuariosAfectadosPorEvento, ref listaGruposAfectadosPorEvento, out recursoPrivado, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                //List<Guid> listaGruposAfectadosPorEvento = ObtenerListaGruposDeProyectoAfectadosPorEvento(pFilaCola, out recursoPrivado);

                DocumentacionCL documentacionCL = new DocumentacionCL(mFicheroConfiguracionBD, mFicheroConfiguracionBD, entityContext, loggingService, redisCacheWrapper, mConfigService, servicesUtilVirtuosoAndReplication);
                documentacionCL.Dominio = mDominio;
                List<Guid> listaPerfilesConRecursosPrivadosProyecto = documentacionCL.PerfilesConRecursosPrivados(pFilaCola.ProyectoId);
                documentacionCL.Dispose();

                IdentidadCN identidadCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                List<Guid> listaPerfilesGruposConRecursosPrivadosProyecto = identidadCN.ObtenerPerfilesProyectoParticipanEnGruposConRecursosPrivados(pFilaCola.ProyectoId);

                ProyectoCL proyCL = new ProyectoCL(entityContext, loggingService, redisCacheWrapper, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                short tipoAcceso = proyCL.ObtenerFilaProyecto(pFilaCola.ProyectoId).TipoAcceso;

                bool proyectoPrivado = tipoAcceso.Equals((short)TipoAcceso.Privado) || tipoAcceso.Equals((short)TipoAcceso.Reservado);

                if (!recursoPrivado)
                {
                    AgregarLiveRecursoPublico(liveUsuariosCL, pFilaCola, listaUsuariosAfectadosPorEvento, listaGruposAfectadosPorEvento, listaPerfilesConRecursosPrivadosProyecto, listaPerfilesGruposConRecursosPrivadosProyecto, proyectoPrivado, nombreCacheElemento, entityContext, loggingService, servicesUtilVirtuosoAndReplication);
                }
                else
                {
                    //// Obtenemos los perfiles que se han quitado de la edicción
                    //List<Guid> listaEditoresEliminadosEdiccionRecursoPrivado = ObtenerEliminadosEdiccionRecursoPrivado(pFilaCola.InfoExtra, LiveUsuariosAD.EDITOR_ELIMINADO);

                    //// Obtenemos los grupos que se han quitado de la edicción
                    //List<Guid> listaGruposEditoresEliminadosEdiccionRecursoPrivado = ObtenerEliminadosEdiccionRecursoPrivado(pFilaCola.InfoExtra, LiveUsuariosAD.GRUPO_EDITORES_ELIMINADO);

                    //UsuarioCN usuCN = new UsuarioCN(mFicheroConfiguracionBD, false);
                    //Dictionary<Guid, List<Guid>> diccionarioGruposIDPerfilesID = usuCN.ObtenerDiccionarioGruposYPerfilesPorListaGruposID(listaGruposEditoresEliminadosEdiccionRecursoPrivado);
                    //usuCN.Dispose();

                    // Obtener perfiles relacionadas con el recurso (editores/lectores) y eliminamos sus cachés
                    DocumentacionCN documentacionCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                    DataWrapperDocumentacion editoresActualesDocumentoDW = documentacionCN.ObtenerEditoresDocumento(mElementoID);
                    Guid perfilPublicadorID = documentacionCN.ObtenerPerfilPublicadorDocumento(mElementoID, pFilaCola.ProyectoId);
                    documentacionCN.Dispose();

                    if (!pFilaCola.IsInfoExtraNull() && pFilaCola.InfoExtra.Contains(Constantes.PRIVACIDAD_CAMBIADA))
                    {
                        // Elimina el evento del Live/Home del proyecto y del Live/Home de mygnoss
                        EliminarLiveProyectoRecursoPrivado(liveUsuariosCL, pFilaCola.ProyectoId, nombreCacheElemento, listaPerfilesConRecursosPrivadosProyecto, perfilPublicadorID, entityContext, loggingService, servicesUtilVirtuosoAndReplication);
                    }


                    // Agrega la caché a los implicados (Funciona OK)
                    AgregarLiveRecursoPrivado(editoresActualesDocumentoDW, pFilaCola, liveUsuariosCL, proyectoPrivado, nombreCacheElemento, entityContext, loggingService, servicesUtilVirtuosoAndReplication);
                }
            }

            liveUsuariosCL.Dispose();
        }

        private void AgregarLiveRecursoPrivado(DataWrapperDocumentacion pEditoresActualesDocumento, LiveUsuariosDS.ColaUsuariosRow pFilaCola, LiveUsuariosCL pLiveUsuariosCL, bool pProyectoPrivado, string pNombreCacheElemento, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            UsuarioCN usuarioCN = new UsuarioCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            IdentidadCN identCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);

            List<Guid> listaUsuarios = new List<Guid>();

            foreach (AD.EntityModel.Models.Documentacion.DocumentoRolIdentidad rolIdentidad in pEditoresActualesDocumento.ListaDocumentoRolIdentidad)
            {
                // Agregar el recurso a cada live del usuario que sea editor.
                Guid? usuarioID = usuarioCN.ObtenerUsuarioIDPorIDPerfil(rolIdentidad.PerfilID);

                if (usuarioID.HasValue && !listaUsuarios.Contains(usuarioID.Value))
                {
                    listaUsuarios.Add(usuarioID.Value);
                }
            }

            foreach (AD.EntityModel.Models.Documentacion.DocumentoRolGrupoIdentidades rolGrupoIdentidades in pEditoresActualesDocumento.ListaDocumentoRolGrupoIdentidades)
            {
                // Agregar el recurso a cada live del usuario que pertenezca al grupo por ser editor.
                List<Guid> usuariosList = usuarioCN.ObtenerUsuariosPertenecenGrupo(rolGrupoIdentidades.GrupoID);

                foreach (Guid usuarioID in usuariosList)
                {
                    if (!listaUsuarios.Contains(usuarioID))
                    {
                        listaUsuarios.Add(usuarioID);
                    }
                }
            }

            foreach (Guid usuarioID in listaUsuarios)
            {
                ActualizarLiveRecursoPublicoUsuario(pLiveUsuariosCL, pFilaCola, mListaAutoresID, mListaOrganizacionesIDAutores, pNombreCacheElemento, pProyectoPrivado, true, usuarioID);
            }

            usuarioCN.Dispose();
            identCN.Dispose();
        }

        private List<Guid> ObtenerEliminadosEdiccionRecursoPrivado(string pInfoExtra, string pClaveDescomposicion)
        {
            List<Guid> listaIDs = new List<Guid>();
            if (pInfoExtra.Contains(pClaveDescomposicion))
            {
                string grupoIDs = pInfoExtra.Substring(pInfoExtra.IndexOf(pClaveDescomposicion) + pClaveDescomposicion.Length);
                grupoIDs = grupoIDs.Substring(0, grupoIDs.LastIndexOf(pClaveDescomposicion));

                string[] delimiter = { "|" };
                foreach (string id in grupoIDs.Split(delimiter, StringSplitOptions.RemoveEmptyEntries))
                {
                    Guid guidTemporal;
                    if (Guid.TryParse(id, out guidTemporal))
                    {
                        listaIDs.Add(guidTemporal);
                    }
                }
            }

            return listaIDs;
        }

        private void AgregarLiveRecursoPublico(LiveUsuariosCL pLiveUsuariosCL, LiveUsuariosDS.ColaUsuariosRow pFilaCola, Dictionary<Guid, List<Guid>> pListaUsuariosAfectadosEventoConRecursosPrivados, Dictionary<Guid, List<Guid>> pListaGruposAfectadosEventoConRecursosPrivados, List<Guid> pListaPerfilesConRecursosPrivados, List<Guid> pListaPerfilesDeGruposConRecursosPrivados, bool pProyectoPrivado, string pNombreCacheElemento, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            //Obtenemos la privacidad del recurso
            DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            bool recursoVisibleSoloParaMiembros = docCN.EsDocumentoEnProyectoPublicoSoloParaMiembros(mElementoID);
            docCN.Dispose();

            List<Guid> listaUsuarios = new List<Guid>();

            //Actualizo la actividad reciente de los usuarios que tienen algún recurso privado.
            foreach (Guid usuarioID in pListaUsuariosAfectadosEventoConRecursosPrivados.Keys)
            {
                // Cada usuario tiene una actividad reciente propia en la Home de la comundidad que se crea cuando el usuario tenga algun recurso privado y se guarda en cache.
                // Si existe la clave de cache, actualizamos su actividad reciente en la Home, si no tiene, buscamos si tiene recursos privados (si no cumple ninguna no se actualiza la actividad reciente de ese usuario).
                List<object> actividadRecientePropia = pLiveUsuariosCL.ObtenerLiveProyectoUsuario(usuarioID, pFilaCola.ProyectoId, "es");

                if (actividadRecientePropia != null && actividadRecientePropia.Count > 0)
                {
                    if (!usuarioID.Equals(Guid.Empty) && !listaUsuarios.Contains(usuarioID))
                    {
                        listaUsuarios.Add(usuarioID);
                    }
                }
                else
                {
                    foreach (Guid perfilID in pListaUsuariosAfectadosEventoConRecursosPrivados[usuarioID])
                    {
                        bool tienePrivados = pListaPerfilesConRecursosPrivados.Contains(perfilID) || pListaPerfilesDeGruposConRecursosPrivados.Contains(perfilID);

                        if (tienePrivados && !usuarioID.Equals(Guid.Empty) && !listaUsuarios.Contains(usuarioID))
                        {
                            listaUsuarios.Add(usuarioID);
                        }
                    }
                }               
            }

            //Actualizo la actividad reciente de los usuarios que pertenecen a algún grupo y tienen algún recurso privado
            UsuarioCN usuCN = new UsuarioCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            foreach (Guid usuarioID in pListaGruposAfectadosEventoConRecursosPrivados.Keys)
            {
                foreach (Guid perfilID in pListaGruposAfectadosEventoConRecursosPrivados[usuarioID])
                {
                    bool tienePrivados = pListaPerfilesConRecursosPrivados.Contains(perfilID) || pListaPerfilesDeGruposConRecursosPrivados.Contains(perfilID);

                    if (tienePrivados)
                    {
                        //Guid? usuarioID = usuCN.ObtenerUsuarioIDPorIDPerfil(perfilID);
                        if (!usuarioID.Equals(Guid.Empty) && !listaUsuarios.Contains(usuarioID))
                        {
                            listaUsuarios.Add(usuarioID);
                        }
                    }
                }
            }
            usuCN.Dispose();

            foreach (Guid usuarioID in listaUsuarios)
            {
                ActualizarLiveRecursoPublicoUsuario(pLiveUsuariosCL, pFilaCola, mListaAutoresID, mListaOrganizacionesIDAutores, pNombreCacheElemento, pProyectoPrivado, recursoVisibleSoloParaMiembros, usuarioID);
            }

            // Agregamos el recurso al live general
            ActualizarLiveRecursoPublicoUsuario(pLiveUsuariosCL, pFilaCola, mListaAutoresID, mListaOrganizacionesIDAutores, pNombreCacheElemento, pProyectoPrivado, recursoVisibleSoloParaMiembros);

            // Inevery Crea: Hay que mantener una caché del proyecto por grupo para que tras registrarse los nuevos usuarios de Santillana Connect se les clone esta actividad reciente
            foreach (Guid grupoID in pListaGruposAfectadosEventoConRecursosPrivados.Keys)
            {
                ActualizarLiveProyectoGrupo(pFilaCola, grupoID, pLiveUsuariosCL, pNombreCacheElemento);
            }
        }

        private void ActualizarLiveRecursoPublicoUsuario(LiveUsuariosCL pLiveUsuariosCL, LiveUsuariosDS.ColaUsuariosRow pFilaCola, List<Guid> mListaAutoresID, List<Guid> mListaOrganizacionesIDAutores, string pNombreCacheElemento, bool proyectoPrivado, bool recursoVisibleSoloParaMiembros, Guid? usuarioid = null)
        {
            if (!usuarioid.HasValue)
            {
                //Actualizo la actividad reciente del perfil del autor del recurso o comentario
                ActualizarLiveProyectoPerfilUsuarioLista(pLiveUsuariosCL, (AccionLive)pFilaCola.Accion, pFilaCola.ProyectoId, mListaAutoresID, pNombreCacheElemento, proyectoPrivado || recursoVisibleSoloParaMiembros);

                //Actualizo la actividad reciente del perfil organización del autor del recurso o comentario
                ActualizarLiveProyectoPerfilOrganizacionLista(pLiveUsuariosCL, pFilaCola, mListaOrganizacionesIDAutores, pNombreCacheElemento, proyectoPrivado || recursoVisibleSoloParaMiembros);
            }
            //Actualizo la actividad reciente de la home de la comunidad
            ActualizarLiveProyectoUsuario(pFilaCola, usuarioid, pLiveUsuariosCL, pNombreCacheElemento, recursoVisibleSoloParaMiembros);
        }

        private void ActualizarLiveProyectoPerfilUsuarioLista(LiveUsuariosCL pLiveUsuariosCL, AccionLive pAccionLiveFilaCola, Guid pProyectoID, List<Guid> pListaPerfilesID, string pNombreCacheElemento, bool pProyectoPrivado)
        {
            foreach (Guid perfilAutorAccion in pListaPerfilesID)
            {
                ActualizarLiveProyectoPerfilUsu(pAccionLiveFilaCola, pProyectoID, perfilAutorAccion, pLiveUsuariosCL, pNombreCacheElemento);
                if (!pProyectoPrivado && pAccionLiveFilaCola != AccionLive.ReprocesarEventoHomeProyecto)
                {
                    ActualizarLiveMetaProyectoPerfilUsu(pAccionLiveFilaCola, perfilAutorAccion, pLiveUsuariosCL, pNombreCacheElemento);
                }
            }
        }

        private void ActualizarLiveProyectoPerfilOrganizacionLista(LiveUsuariosCL pLiveUsuariosCL, LiveUsuariosDS.ColaUsuariosRow pFilaCola, List<Guid> mListaAutoresID, string pNombreCacheElemento, bool pProyectoPrivado, Guid? pUsuarioID = null)
        {
            if (pFilaCola.Accion != (short)AccionLive.ReprocesarEventoHomeProyecto)
            {
                foreach (Guid organizacionID in mListaOrganizacionesIDAutores)
                {
                    ActualizarLiveProyectoPerfilOrg(pFilaCola, organizacionID, pLiveUsuariosCL, pNombreCacheElemento);
                    if (!pProyectoPrivado)
                    {
                        ActualizarLiveMetaProyectoPerfilOrg(pFilaCola, organizacionID, pLiveUsuariosCL, pNombreCacheElemento);
                    }
                }
            }
        }

        private void EliminarLiveProyectoRecursoPrivado(LiveUsuariosCL pLiveUsuariosCL, Guid pProyectoID, string pNombreCacheElemento, List<Guid> pListaPerfilesConRecursosPrivados, Guid pPerfilPublicadorID, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            //Lo eliminamos del perfil del publicador
            pLiveUsuariosCL.EliminarLiveProyectoPerfilUsuario(pPerfilPublicadorID, pProyectoID, pNombreCacheElemento);

            //Quitamos el recurso de todas las personas que lo tienen en su cola
            pLiveUsuariosCL.EliminarLiveProyectoUsuario(null, pProyectoID, pNombreCacheElemento);
            pLiveUsuariosCL.EliminarLiveProyectoUsuarioInvitado(pProyectoID, pNombreCacheElemento);

            pLiveUsuariosCL.EliminarLiveProyectoUsuario(null, ProyectoAD.MetaProyecto, pNombreCacheElemento);

            if (pListaPerfilesConRecursosPrivados.Count > 0)
            {
                PersonaCN persCN = new PersonaCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                List<Guid> listaUsuariosID = persCN.ObtenerUsuariosIDDeListaPerfil(pListaPerfilesConRecursosPrivados);
                foreach (Guid usuarioID in listaUsuariosID)
                {
                    pLiveUsuariosCL.EliminarLiveProyectoUsuario(usuarioID, pProyectoID, pNombreCacheElemento);
                }
            }

            //Quitamos el recurso de todas los grupos que lo tienen en su cola
            UsuarioCN usuarioCN = new UsuarioCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            IdentidadCN identCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            List<Guid> listaTodosGruposProyecto = usuarioCN.ObtenerListaGruposPorProyecto(pProyectoID);

            //Al ser una clave de caché privada para cada grupo, hay que obtener los grupos si o si.
            foreach (Guid grupoID in listaTodosGruposProyecto)
            {
                pLiveUsuariosCL.EliminarLiveProyectoGrupo(grupoID, pProyectoID, pNombreCacheElemento);

                //Borrar cada usuario que pertenece al grupo
                List<Guid> perfilesParticipantesGrupo = identCN.ObtenerPerfilesParticipantesGrupo(grupoID);
                foreach (Guid perfilParticipanteGrupo in perfilesParticipantesGrupo)
                {
                    if (perfilParticipanteGrupo != pPerfilPublicadorID)
                    {
                        Guid? usuarioID = usuarioCN.ObtenerUsuarioIDPorIDPerfil(perfilParticipanteGrupo);
                        pLiveUsuariosCL.EliminarLiveProyectoUsuario(usuarioID, pProyectoID, pNombreCacheElemento);
                    }
                }
            }
            identCN.Dispose();
            usuarioCN.Dispose();
        }

        public void ObtenerListaUsuariosDeProyectoAfectadosPorEvento(LiveUsuariosDS.ColaUsuariosRow pFilaCola, ref Dictionary<Guid, List<Guid>> pListaUsuariosAfectados, ref Dictionary<Guid, List<Guid>> pListaGruposAfectados, out bool pDocumentoPrivadoEditores, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            //Obtenemos la privacidad del recurso
            DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            pDocumentoPrivadoEditores = docCN.EsDocumentoEnProyectoPrivadoEditores(mElementoID, pFilaCola.ProyectoId);
            docCN.Dispose();

            /*Agregar o eliminar usuarios por su scope*/
            IdentidadCN identidadCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            Guid? perfilID = null;
            if (pFilaCola.ProyectoId.Equals(ProyectoAD.MetaProyecto))
            {
                Guid perfil = Guid.Empty;
                Guid.TryParse(pFilaCola.InfoExtra, out perfil);
                if (perfil != Guid.Empty)
                {
                    perfilID = perfil;
                }
            }
            else
            {
                perfilID = identidadCN.ObtenerPerfilIDPublicadorRecursoEnProyecto(mElementoID, pFilaCola.ProyectoId);
            }

            if (perfilID.HasValue)
            {
                if (!mListaAutoresID.Contains(perfilID.Value))
                {
                    mListaAutoresID.Add(perfilID.Value);
                }

                Guid? organizacionID = identidadCN.ObtenerOrganizacionIDConPerfilID(perfilID.Value);
                if (organizacionID.HasValue)
                {
                    mListaOrganizacionesIDAutores.Add(organizacionID.Value);
                }
            }

            AccionLive accion = (AccionLive)pFilaCola.Accion;

            switch (accion)
            {
                case AccionLive.ComentarioAgregado:
                    Guid? perfilUsuarioComenta = identidadCN.ObtenerPerfilIDPublicadorComentarioEnRecurso(pFilaCola.Id);

                    if (perfilUsuarioComenta.HasValue)
                    {
                        mListaAutoresID.Clear();
                        mListaAutoresID.Add(perfilUsuarioComenta.Value);
                    }
                    break;
                case AccionLive.Votado:
                    Guid? perfilUsuarioVota = identidadCN.ObtenerPerfilIDPublicadorVotoEnRecurso(pFilaCola.Id);
                    if (perfilUsuarioVota.HasValue)
                    {
                        mListaAutoresID.Clear();
                        mListaAutoresID.Add(perfilUsuarioVota.Value);
                    }
                    break;
            }
            identidadCN.Dispose();

            UsuarioCN usuarioCN = new UsuarioCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            if (pDocumentoPrivadoEditores)
            {
                pListaUsuariosAfectados = usuarioCN.ObtenerDiccionarioUsuariosYPerfilesPorProyectoYDocPrivado(pFilaCola.ProyectoId, mElementoID);
                pListaGruposAfectados = usuarioCN.ObtenerDiccionarioGruposYPerfilesPorProyectoYDocPrivado(pFilaCola.ProyectoId, mElementoID);
            }
            else
            {
                pListaUsuariosAfectados = usuarioCN.ObtenerDiccionarioUsuariosYPerfilesPorProyectoYDocPrivado(pFilaCola.ProyectoId, null);
                pListaGruposAfectados = usuarioCN.ObtenerDiccionarioGruposYPerfilesPorProyectoYDocPrivado(pFilaCola.ProyectoId, null);
            }

            usuarioCN.Dispose();
        }

        public List<Guid> ObtenerListaGruposDeProyectoAfectadosPorEvento(LiveUsuariosDS.ColaUsuariosRow pFilaCola, out bool pDocumentoPrivadoEditores, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            List<Guid> listaGrupos = new List<Guid>();

            UsuarioCN usuarioCN = new UsuarioCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            IdentidadCN identidadCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);

            //Obtenemos la privacidad del recurso
            DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            pDocumentoPrivadoEditores = docCN.EsDocumentoEnProyectoPrivadoEditores(mElementoID, pFilaCola.ProyectoId);
            docCN.Dispose();

            /*Agregar o eliminar usuarios por su scope*/

            Guid? perfilID = null;
            if (pFilaCola.ProyectoId.Equals(ProyectoAD.MetaProyecto))
            {
                Guid perfil = Guid.Empty;
                Guid.TryParse(pFilaCola.InfoExtra, out perfil);
                if (perfil != Guid.Empty)
                {
                    perfilID = perfil;
                }
            }
            else
            {
                perfilID = identidadCN.ObtenerPerfilIDPublicadorRecursoEnProyecto(mElementoID, pFilaCola.ProyectoId);
            }

            if (perfilID.HasValue)
            {
                if (!mListaAutoresID.Contains(perfilID.Value))
                {
                    mListaAutoresID.Add(perfilID.Value);
                }

                Guid? organizacionID = identidadCN.ObtenerOrganizacionIDConPerfilID(perfilID.Value);
                if (organizacionID.HasValue)
                {
                    mListaOrganizacionesIDAutores.Add(organizacionID.Value);
                }
            }

            AccionLive accion = (AccionLive)pFilaCola.Accion;

            switch (accion)
            {
                case AccionLive.ComentarioAgregado:
                    Guid? perfilUsuarioComenta = identidadCN.ObtenerPerfilIDPublicadorComentarioEnRecurso(pFilaCola.Id);

                    if (perfilUsuarioComenta.HasValue)
                    {
                        mListaAutoresID.Clear();
                        mListaAutoresID.Add(perfilUsuarioComenta.Value);
                    }
                    break;
                case AccionLive.Votado:
                    Guid? perfilUsuarioVota = identidadCN.ObtenerPerfilIDPublicadorVotoEnRecurso(pFilaCola.Id);
                    if (perfilUsuarioVota.HasValue)
                    {
                        mListaAutoresID.Clear();
                        mListaAutoresID.Add(perfilUsuarioVota.Value);
                    }
                    break;
            }

            // Devolvemos tdos los grupos de la comunidad.

            //if (pDocumentoPrivadoEditores)
            //{
            //    listaGrupos = usuarioCN.ObtenerListaGruposPorProyectoYDocPrivado(pFilaCola.ProyectoId, mElementoID);
            //}
            //else
            //{
            //listaGrupos = usuarioCN.ObtenerListaUsuariosYPerfilPorProyecto(pFilaCola.ProyectoId);
            listaGrupos = usuarioCN.ObtenerListaGruposPorProyecto(pFilaCola.ProyectoId);
            //}
            usuarioCN.Dispose();
            identidadCN.Dispose();

            return listaGrupos;
        }

        private void ObtenerIDElementoPrincipal(LiveUsuariosDS.ColaUsuariosRow pFilaCola,EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            TipoLive tipo = (TipoLive)pFilaCola.Tipo;
            AccionLive accion = (AccionLive)pFilaCola.Accion;

            mElementoID = pFilaCola.Id;

            switch (tipo)
            {
                case TipoLive.Recurso:
                case TipoLive.Pregunta:
                case TipoLive.Debate:
                    DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);

                    switch (accion)
                    {
                        case AccionLive.ComentarioAgregado:
                            mElementoID = docCN.ObtenerIDDocumentoDeComentarioPorID(pFilaCola.Id);
                            break;
                        case AccionLive.Votado:
                            mElementoID = docCN.ObtenerIDDocumentoDeVotoPorID(pFilaCola.Id);
                            break;
                    }
                    docCN.Dispose();
                    break;
            }
        }

        /// <summary>
        /// Obtiene el último Score que se asigno a un perfil
        /// </summary>
        /// <param name="pPerfilID"></param>
        /// <returns></returns>
        private int ObtenerUltimoScorePerfil(Guid pPerfilID)
        {
            if (!mListaScorePorPerfil.ContainsKey(pPerfilID))
            {
                //No tenemos datos sobre este perfil, metemos -2 para devolver -1
                mListaScorePorPerfil.Add(pPerfilID, -2);
            }
            return ++mListaScorePorPerfil[pPerfilID];
        }

        /// <summary>
        /// Obtiene el último Score que se asigno a un usuario de proyecto
        /// </summary>
        /// <param name="pProyectoID"></param>
        /// <param name="pUsuarioID"></param>
        /// <returns></returns>
        private int ObtenerUltimoScoreProyectoUsuarioSuscripciones(Guid pProyectoID, Guid pUsuarioID)
        {
            string claveProyUsuSuscr = "suscripciones_" + pProyectoID + pUsuarioID;

            if (!mListaScorePorProyUsuSuscr.ContainsKey(claveProyUsuSuscr))
            {
                //No tenemos datos sobre este perfil, metemos -2 para devolver -1
                mListaScorePorProyUsuSuscr.Add(claveProyUsuSuscr, -2);
            }
            return ++mListaScorePorProyUsuSuscr[claveProyUsuSuscr];
        }

        /// <summary>
        /// Obtiene el último Score que se asigno a un usuario de proyecto
        /// </summary>
        /// <param name="pProyectoID"></param>
        /// <param name="pUsuarioID"></param>
        /// <returns></returns>
        private int ObtenerUltimoScoreUsuarioSuscripciones(Guid pUsuarioID)
        {
            string claveUsuSuscr = "suscripcionesUsu_" + pUsuarioID;

            if (!mListaScorePorUsuSuscr.ContainsKey(claveUsuSuscr))
            {
                //No tenemos datos sobre este perfil, metemos -2 para devolver -1
                mListaScorePorUsuSuscr.Add(claveUsuSuscr, -2);
            }
            return ++mListaScorePorUsuSuscr[claveUsuSuscr];
        }

        /// <summary>
        /// Obtiene el último Score que se asigno a un usuario de proyecto
        /// </summary>
        /// <param name="pProyectoID"></param>
        /// <param name="pUsuarioID"></param>
        /// <returns></returns>
        private int ObtenerUltimoScoreProyectoUsuario(Guid pProyectoID, Guid? pUsuarioID)
        {
            string claveProyUsu = pProyectoID.ToString();
            if (pUsuarioID.HasValue && !pUsuarioID.Equals(Guid.Empty))
            {
                claveProyUsu += pUsuarioID.Value;
            }

            if (!mListaScorePorProyUsu.ContainsKey(claveProyUsu))
            {
                //No tenemos datos sobre este perfil, metemos -2 para devolver -1
                mListaScorePorProyUsu.Add(claveProyUsu, -2);
            }
            return ++mListaScorePorProyUsu[claveProyUsu];
        }

        /// <summary>
        /// Obtiene el último Score que se asigno a un usuario de proyecto
        /// </summary>
        /// <param name="pProyectoID"></param>
        /// <param name="pUsuarioID"></param>
        /// <returns></returns>
        private int ObtenerUltimoScoreProyectoGrupo(Guid pProyectoID, Guid? pGrupoID)
        {
            string claveProyGru = pProyectoID.ToString();
            if (pGrupoID.HasValue)
            {
                claveProyGru += pGrupoID.Value;
            }

            if (!mListaScorePorProyGru.ContainsKey(claveProyGru))
            {
                //No tenemos datos sobre este perfil, metemos -2 para devolver -1
                mListaScorePorProyGru.Add(claveProyGru, -2);
            }
            return ++mListaScorePorProyGru[claveProyGru];
        }

        /// <summary>
        /// Obtiene el último Score que se asigno a un usuario de proyecto
        /// </summary>
        /// <param name="pProyectoID"></param>
        /// <param name="pUsuarioID"></param>
        /// <returns></returns>
        private int ObtenerUltimoScoreProyectoPerfilUsuario(Guid pProyectoID, Guid pPerfilID)
        {
            string claveProyPerfilUsu = string.Concat(pProyectoID, pPerfilID);

            if (!mListaScorePorProyPerfilUsu.ContainsKey(claveProyPerfilUsu))
            {
                //No tenemos datos sobre este perfil, metemos -2 para devolver -1
                mListaScorePorProyPerfilUsu.Add(claveProyPerfilUsu, -2);
            }
            return ++mListaScorePorProyPerfilUsu[claveProyPerfilUsu];
        }


        /// <summary>
        /// Obtiene el último Score que se asigno a un usuario de proyecto
        /// </summary>
        /// <param name="pProyectoID"></param>
        /// <param name="pUsuarioID"></param>
        /// <returns></returns>
        private int ObtenerUltimoScoreProyectoPerfilOrg(Guid pProyectoID, Guid pOrganizacionID)
        {
            string claveProyPerfilUsu = string.Concat(pProyectoID, pOrganizacionID);

            if (!mListaScorePorProyPerfilOrg.ContainsKey(claveProyPerfilUsu))
            {
                //No tenemos datos sobre este perfil, metemos -2 para devolver -1
                mListaScorePorProyPerfilOrg.Add(claveProyPerfilUsu, -2);
            }
            return ++mListaScorePorProyPerfilOrg[claveProyPerfilUsu];
        }

        #endregion
        /// <summary>
        /// Indica si hay que procesar el elemento de la cola para crear eventos de la home de la comunidad.
        /// </summary>
        /// <param name="pColaRow">Fila de cola</param>
        /// <param name="pProyectoDS">DataSet con las filas de los tipos omitidos cargadas</param>
        /// <returns>TRUE si hay que procesar el elemento de la cola para crear eventos de la home de la comunidad, FALSE en caso contrario</returns>
        private bool HayQueProcesarEvento(Guid recursoID, LiveUsuariosDS.ColaUsuariosRow pColaRow, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
           
            return proyCN.ComprobarSiRecursoSePublicaEnActividadReciente(recursoID, pColaRow.ProyectoId);
            
        }
    }

}
