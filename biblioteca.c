#define _POSIX_C_SOURCE 200809L
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <semaphore.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <stdbool.h>
#include <mqueue.h>
#include <string.h>
#include <sys/shm.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <errno.h>
#include <time.h>
#include <stdatomic.h>

pid_t pid_mostrador, pid_deposito;

#define NOMBRE_COLA "/cola_biblioteca"
#define TAM_MENSAJE 64

mqd_t cola;

sem_t sem_extraccion;
sem_t sem_registro;

#define NOMBRE_MEMORIA "/memoria_biblioteca"

int fd_memoria;
char *memoria_compartida;

// Estado local de parada, compartido entre los hilos de cada proceso.
atomic_bool parar = false;
atomic_bool fallo = false;
sigset_t interrupcion;

int tiempo_aleatorio(int min, int max) {
    return rand() % (max - min + 1) + min;
}

// Recoger SIGINT pendiente sin bloquear el hilo principal.
bool comprobar_parada(void) {
    struct timespec cero = {0, 0};
    int resultado = sigtimedwait(&interrupcion, NULL, &cero);
    if (resultado == SIGINT) {
        atomic_store(&parar, true);
    } else if (resultado == -1 && errno != EAGAIN && errno != EINTR) {
        perror("Error al comprobar SIGINT");
        atomic_store(&fallo, true);
        atomic_store(&parar, true);
    }
    return atomic_load(&parar);
}

// Las operaciones de cola esperan como máximo un segundo por intento.
struct timespec limite_espera(void) {
    struct timespec limite;
    if (clock_gettime(CLOCK_REALTIME, &limite) == -1) {
        perror("Error al consultar el reloj");
        exit(EXIT_FAILURE);
    }
    limite.tv_sec++;
    return limite;
}

// Comprobar los errores de las operaciones de pthread.
bool comprobar_hilo(int error, const char *operacion) {
    if (error == 0) return true;
    fprintf(stderr, "%s: %s\n", operacion, strerror(error));
    atomic_store(&fallo, true);
    atomic_store(&parar, true);
    return false;
}

// Un fallo de semáforo impide garantizar el orden: finalizar el proceso.
void esperar_semaforo(sem_t *sem) {
    int resultado;
    do {
        resultado = sem_wait(sem);
    } while (resultado == -1 && errno == EINTR);
    if (resultado == -1) {
        perror("Error en sem_wait");
        exit(EXIT_FAILURE);
    }
}

void liberar_semaforo(sem_t *sem) {
    if (sem_post(sem) == -1) {
        perror("Error en sem_post");
        exit(EXIT_FAILURE);
    }
}

void* busqueda(void* arg) {
    printf("[Hilo Búsqueda] Comienzo mi ejecución...\n");

    printf("[Hilo Búsqueda] Buscando ejemplar %s...\n", (char *)arg);
    sleep(tiempo_aleatorio(1, 3));
    printf("[Hilo Búsqueda] Ejemplar %s localizado. Enviando a extracción...\n", (char *)arg);

    liberar_semaforo(&sem_extraccion);

    return NULL;
}

void* extraccion(void* arg) {
    esperar_semaforo(&sem_extraccion);

    printf("[Hilo Extracción] Comienzo mi ejecución...\n");

    printf("[Hilo Extracción] Extrayendo ejemplar %s de la estantería...\n", (char *)arg);
    sleep(tiempo_aleatorio(2, 4));
    printf("[Hilo Extracción] Ejemplar %s extraído. Enviando a registro...\n", (char *)arg);

    liberar_semaforo(&sem_registro);

    return NULL;
}

void* registro(void* arg) {
    esperar_semaforo(&sem_registro);

    printf("[Hilo Registro] Comienzo mi ejecución...\n");

    printf("[Hilo Registro] Registrando préstamo del ejemplar %s...\n", (char *)arg);
    sleep(tiempo_aleatorio(1, 2));
    printf("[Hilo Registro] Préstamo del ejemplar %s registrado. Notificando al mostrador...\n", (char *)arg);

    return NULL;
}

void* recibir_notificaciones(void* arg) {
    (void)arg;

    sigset_t señales;
    siginfo_t informacion;
    struct timespec espera = {1, 0};

    sigemptyset(&señales);
    sigaddset(&señales, SIGUSR1);

    while (!atomic_load(&parar)) {

        // Esperar la señal del Depósito, revisando periódicamente la parada.
        if (sigtimedwait(&señales, &informacion, &espera) == SIGUSR1) {

            // Leer el identificador de la memoria compartida
            printf("[Proceso Mostrador] Recibida señal SIGUSR1.\n");

            printf("[Proceso Mostrador] Ejemplar %s preparado para entregar al usuario.\n",
                   memoria_compartida);

            // Confirmar al Depósito que se ha leído el ejemplar
            if (kill(informacion.si_pid, SIGUSR2) == -1) {
                if (!atomic_load(&parar) && errno != ESRCH) {
                    perror("Error al enviar SIGUSR2");
                    atomic_store(&fallo, true);
                    atomic_store(&parar, true);
                }
            }

        } else if (errno != EINTR && errno != EAGAIN) {
            perror("Error al recibir SIGUSR1");
            atomic_store(&fallo, true);
            atomic_store(&parar, true);
        }
    }

    return NULL;
}

// Cada proceso cierra sus propios descriptores y mapeos.
void cerrar_recursos(void) {
    if (mq_close(cola) == -1) { perror("Error en mq_close"); atomic_store(&fallo, true); }
    if (munmap(memoria_compartida, TAM_MENSAJE) == -1) { perror("Error en munmap"); atomic_store(&fallo, true); }
}

// Solo el padre elimina los nombres de los recursos compartidos.
void eliminar_recursos(void) {
    cerrar_recursos();
    if (mq_unlink(NOMBRE_COLA) == -1) { perror("Error en mq_unlink"); atomic_store(&fallo, true); }
    if (shm_unlink(NOMBRE_MEMORIA) == -1) { perror("Error en shm_unlink"); atomic_store(&fallo, true); }
}

void solicitar_parada(pid_t pid) {
    if (pid > 0 && kill(pid, SIGINT) == -1 && errno != ESRCH) {
        perror("Error al enviar SIGINT");
        atomic_store(&fallo, true);
    }
}

int main(int argc, char* argv[]) {

    bool prueba_saturacion = argc == 2 && strcmp(argv[1], "--prueba-saturacion") == 0;
    if (argc > 1 && !prueba_saturacion) {
        fprintf(stderr, "Uso: %s [--prueba-saturacion]\n", argv[0]);
        return EXIT_FAILURE;
    }
    setvbuf(stdout, NULL, _IOLBF, 0);

    // Bloquear las señales antes de crear recursos, procesos e hilos.
    sigset_t señales;
    sigemptyset(&señales);
    sigaddset(&señales, SIGUSR1);
    sigaddset(&señales, SIGUSR2);
    sigaddset(&señales, SIGINT);
    sigaddset(&señales, SIGCHLD);
    sigemptyset(&interrupcion);
    sigaddset(&interrupcion, SIGINT);

    if (sigprocmask(SIG_BLOCK, &señales, NULL) == -1) {
        perror("Error al bloquear las señales");
        exit(EXIT_FAILURE);
    }

    struct mq_attr atributos = {0};

    atributos.mq_flags = 0;
    atributos.mq_maxmsg = 10;
    atributos.mq_msgsize = TAM_MENSAJE;
    atributos.mq_curmsgs = 0;

    // No eliminar recursos de otra ejecución que pueda estar activa.
    cola = mq_open(NOMBRE_COLA, O_CREAT | O_EXCL | O_RDWR, 0600, &atributos);

    if (cola == (mqd_t)-1) {
        perror("Error al crear la cola de mensajes");
        exit(EXIT_FAILURE);
    }

    // Crear la memoria compartida
    fd_memoria = shm_open(NOMBRE_MEMORIA, O_CREAT | O_EXCL | O_RDWR, 0600);

    if (fd_memoria == -1) {
        perror("Error al crear la memoria compartida");
        mq_close(cola);
        mq_unlink(NOMBRE_COLA);
        exit(EXIT_FAILURE);
    }

    // Establecer el tamaño de la memoria compartida
    if (ftruncate(fd_memoria, TAM_MENSAJE) == -1) {
        perror("Error al establecer el tamaño de la memoria");
        close(fd_memoria);
        shm_unlink(NOMBRE_MEMORIA);
        mq_close(cola);
        mq_unlink(NOMBRE_COLA);
        exit(EXIT_FAILURE);
    }

    // Mapear la memoria compartida
    memoria_compartida = mmap(NULL, TAM_MENSAJE,
                            PROT_READ | PROT_WRITE,
                            MAP_SHARED, fd_memoria, 0);

    if (memoria_compartida == MAP_FAILED) {
        perror("Error al mapear la memoria compartida");
        close(fd_memoria);
        shm_unlink(NOMBRE_MEMORIA);
        mq_close(cola);
        mq_unlink(NOMBRE_COLA);
        exit(EXIT_FAILURE);
    }

    // Inicializar la memoria compartida
    memoria_compartida[0] = '\0';

    if (close(fd_memoria) == -1) {
        perror("Error al cerrar el descriptor de memoria");
        eliminar_recursos();
        exit(EXIT_FAILURE);
    }

    pid_mostrador = fork();

    if (pid_mostrador == -1) {
        perror("Error al crear el Mostrador");
        eliminar_recursos();
        exit(EXIT_FAILURE);
    }

    if (pid_mostrador != 0) {
        pid_deposito = fork();
        if (pid_deposito == -1) {
            perror("Error al crear el Depósito");
            solicitar_parada(pid_mostrador);
            while (waitpid(pid_mostrador, NULL, 0) == -1 && errno == EINTR) {}
            eliminar_recursos();
            exit(EXIT_FAILURE);
        }
        if (pid_deposito != 0) {
            /* Proceso padre */

            printf("[Proceso Padre] Esperando a que terminen mis hijos...\n");

            sigset_t control;
            sigemptyset(&control);
            sigaddset(&control, SIGINT);
            sigaddset(&control, SIGCHLD);

            // Esperar SIGINT o la finalización de cualquiera de los hijos.
            while (pid_mostrador > 0 || pid_deposito > 0) {
                int señal = sigwaitinfo(&control, NULL);
                if (señal == -1 && errno == EINTR) continue;
                if (señal == SIGINT) {
                    if (!atomic_exchange(&parar, true))
                        printf("[Proceso Padre] SIGINT recibido. Finalizando...\n");
                    solicitar_parada(pid_mostrador);
                    solicitar_parada(pid_deposito);
                } else if (señal == -1) {
                    perror("Error al esperar señales en el padre");
                    atomic_store(&fallo, true);
                    solicitar_parada(pid_mostrador);
                    solicitar_parada(pid_deposito);
                }

                int estado;
                pid_t hijo;
                while ((hijo = waitpid(-1, &estado, WNOHANG)) > 0) {
                    if (hijo == pid_mostrador) pid_mostrador = 0;
                    if (hijo == pid_deposito) pid_deposito = 0;
                    if (!WIFEXITED(estado) || WEXITSTATUS(estado) != 0)
                        atomic_store(&fallo, true);
                    // Si un hijo termina, solicitar también la parada del otro.
                    solicitar_parada(pid_mostrador);
                    solicitar_parada(pid_deposito);
                }
                if (hijo == -1 && errno == ECHILD) {
                    pid_mostrador = pid_deposito = 0;
                } else if (hijo == -1 && errno != EINTR) {
                    perror("Error en waitpid");
                    atomic_store(&fallo, true);
                }
            }

            eliminar_recursos();
            printf("[Proceso Padre] Limpieza de cola y memoria compartida finalizada.\n");

            printf("[Proceso Padre] Ejecución finalizada.\n");
        } else {
            /* Proceso Depósito */
            printf("[Proceso Depósito] Comienzo mi ejecución...\n");

            char mensaje[TAM_MENSAJE];

            pthread_t hilo_busqueda, hilo_extraccion, hilo_registro;

            if (sem_init(&sem_extraccion, 0, 0) == -1) {
                perror("Error al inicializar sem_extraccion");
                cerrar_recursos();
                exit(EXIT_FAILURE);
            }
            if (sem_init(&sem_registro, 0, 0) == -1) {
                perror("Error al inicializar sem_registro");
                sem_destroy(&sem_extraccion);
                cerrar_recursos();
                exit(EXIT_FAILURE);
            }

            // Preparar la recepción de la confirmación del Mostrador
            sigset_t confirmacion;

            sigemptyset(&confirmacion);
            sigaddset(&confirmacion, SIGUSR2);

            while (!comprobar_parada()) {

                struct timespec limite = limite_espera();
                if (mq_timedreceive(cola, mensaje, TAM_MENSAJE, NULL, &limite) != -1) {

                    if (comprobar_parada()) break;
                    mensaje[TAM_MENSAJE - 1] = '\0';
                    printf("[Proceso Depósito] Recibida solicitud del ejemplar %s.\n", mensaje);

                    // Creación de los tres hilos (solo unir los que se crearon).
                    bool creado_busqueda = comprobar_hilo(pthread_create(&hilo_busqueda, NULL, busqueda, mensaje), "Error al crear búsqueda");
                    bool creado_extraccion = creado_busqueda && comprobar_hilo(pthread_create(&hilo_extraccion, NULL, extraccion, mensaje), "Error al crear extracción");
                    bool creado_registro = creado_extraccion && comprobar_hilo(pthread_create(&hilo_registro, NULL, registro, mensaje), "Error al crear registro");

                    // Esperamos a que finalicen los tres hilos
                    if (creado_busqueda && !comprobar_hilo(pthread_join(hilo_busqueda, NULL), "Error al esperar búsqueda")) exit(EXIT_FAILURE);
                    if (creado_extraccion && !comprobar_hilo(pthread_join(hilo_extraccion, NULL), "Error al esperar extracción")) exit(EXIT_FAILURE);
                    if (creado_registro && !comprobar_hilo(pthread_join(hilo_registro, NULL), "Error al esperar registro")) exit(EXIT_FAILURE);

                    // Si se pidió parar, terminar la solicitud en curso sin esperar confirmación.
                    if (comprobar_parada()) break;

                    // Escribir el identificador en memoria compartida
                    strcpy(memoria_compartida, mensaje);

                    printf("[Proceso Depósito] Ejemplar %s escrito en memoria compartida.\n",
                        memoria_compartida);

                    // Notificar al Mostrador
                    printf("[Proceso Depósito] Enviando SIGUSR1 al Mostrador.\n");

                    if (kill(pid_mostrador, SIGUSR1) == -1) {
                        if (errno != ESRCH) {
                            perror("Error al enviar SIGUSR1");
                            atomic_store(&fallo, true);
                        }
                        break;
                    }

                    // Esperar a que el Mostrador confirme la lectura
                    int resultado = -1;
                    struct timespec espera = {1, 0};
                    siginfo_t informacion;

                    while (!comprobar_parada()) {
                        resultado = sigtimedwait(&confirmacion, &informacion, &espera);
                        if (resultado == SIGUSR2 && informacion.si_pid == pid_mostrador) break;
                        if (resultado == -1 && errno != EINTR && errno != EAGAIN) {
                            perror("Error al recibir SIGUSR2");
                            atomic_store(&fallo, true);
                            atomic_store(&parar, true);
                            break;
                        }
                    }
                    if (comprobar_parada()) break;

                    printf("[Proceso Depósito] Confirmación SIGUSR2 recibida.\n");

                    printf("[Proceso Depósito] Solicitud del ejemplar %s completada.\n", mensaje);

                } else if (errno != ETIMEDOUT && errno != EINTR) {
                    perror("Error al recibir la solicitud");
                    atomic_store(&fallo, true);
                    break;
                }
            }

            // Los hilos ya han terminado: destruir los semáforos.
            if (sem_destroy(&sem_extraccion) == -1) { perror("Error al destruir sem_extraccion"); atomic_store(&fallo, true); }
            if (sem_destroy(&sem_registro) == -1) { perror("Error al destruir sem_registro"); atomic_store(&fallo, true); }
            cerrar_recursos();
            printf("[Proceso Depósito] Ejecución finalizada.\n");
        }
    } else {
        /* Proceso Mostrador */
        printf("[Proceso Mostrador] Comienzo mi ejecución...\n");
        int num_solicitud = 0;
        char mensaje[TAM_MENSAJE];

        // Crear el hilo encargado de recibir las notificaciones
        pthread_t hilo_notificaciones;

        int error = pthread_create(&hilo_notificaciones, NULL,
                                recibir_notificaciones, NULL);

        if (!comprobar_hilo(error, "Error al crear el hilo de notificaciones")) {
            cerrar_recursos();
            exit(EXIT_FAILURE);
        }

        while (!comprobar_parada()) {

            // Esperar en intervalos cortos para poder atender SIGINT.
            int segundos = prueba_saturacion ? 0 : tiempo_aleatorio(3, 8);
            for (int i = 0; i < segundos && !comprobar_parada(); i++) sleep(1);
            if (comprobar_parada()) break;
            printf("[Proceso Mostrador] Recibida solicitud de préstamo nº %d. Enviando al depósito...\n", num_solicitud);

            snprintf(mensaje, TAM_MENSAJE, "%d", num_solicitud);

            // Si la cola está llena, esperar y reintentar el mismo mensaje.
            int resultado = -1;
            while (!comprobar_parada()) {
                struct timespec limite = limite_espera();
                resultado = mq_timedsend(cola, mensaje, strlen(mensaje) + 1, 0, &limite);
                if (resultado == 0) break;
                if (errno != ETIMEDOUT && errno != EINTR) {
                    perror("Error al enviar la solicitud");
                    atomic_store(&fallo, true);
                    atomic_store(&parar, true);
                    break;
                }
            }
            if (resultado != 0) break;

            if (prueba_saturacion) {
                struct mq_attr estado_cola;
                if (mq_getattr(cola, &estado_cola) == -1) {
                    perror("Error al consultar la cola");
                    atomic_store(&fallo, true);
                    break;
                }
                printf("[Cola] Mensajes pendientes: %ld/%ld\n", estado_cola.mq_curmsgs, estado_cola.mq_maxmsg);
            }

            num_solicitud++;

        }

        atomic_store(&parar, true);
        if (!comprobar_hilo(pthread_join(hilo_notificaciones, NULL), "Error al esperar notificaciones")) exit(EXIT_FAILURE);
        cerrar_recursos();
        printf("[Proceso Mostrador] Ejecución finalizada.\n");
    }

    exit(atomic_load(&fallo) ? EXIT_FAILURE : EXIT_SUCCESS);
}
