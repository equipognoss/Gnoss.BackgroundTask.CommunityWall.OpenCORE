![](https://content.gnoss.ws/imagenes/proyectos/personalizacion/7e72bf14-28b9-4beb-82f8-e32a3b49d9d3/cms/logognossazulprincipal.png)

# Gnoss.BackgroundTask.CommunityWall.OpenCORE

Aplicación de segundo plano que se encarga de generar la actividad reciente de cada comunidad.

Este servicio está escuchando la cola de nombre "ColaUsuarios". Se envía un mensaje a esta cola cada vez que se crea, comparte, edita o elimina un recurso; se crea, edita o elimina un comentario; se da de alta, edita su perfil o se da de baja una persona de una comunidad desde la Web o el API, para que este servicio genere el muro de actividad reciente de una comunidad.

Configuración estandar de esta aplicación en el archivo docker-compose.yml: 

```yml
communitywall:
    image: gnoss/communitywall
    env_file: .env
    environment:
     virtuosoConnectionString: ${virtuosoConnectionString}
     acid: ${acid}
     base: ${base}
     RabbitMQ__colaServiciosWin: ${RabbitMQ}
     RabbitMQ__colaReplicacion: ${RabbitMQ}
     redis__redis__ip__master: ${redis__redis__ip__master}
     redis__redis__bd: ${redis__redis__bd}
     redis__redis__timeout: ${redis__redis__timeout}
     redis__recursos__ip__master: ${redis__recursos__ip__master}
     redis__recursos__bd: ${redis__recursos_bd}
     redis__recursos__timeout: ${redis__recursos_timeout}
     redis__liveUsuarios__ip__master: ${redis__liveUsuarios__ip__master}
     redis__liveUsuarios__bd: ${redis__liveUsuarios_bd}
     redis__liveUsuarios__timeout: ${redis__liveUsuarios_timeout}
     idiomas: "es|Español,en|English"
     connectionType: "0"
     intervalo: "100"
    volumes:
     - ./logs/communitywall:/app/logs

```

Se pueden consultar los posibles valores de configuración de cada parámetro aquí: https://github.com/equipognoss/Gnoss.Platform.Deploy
