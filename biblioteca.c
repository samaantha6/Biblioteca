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

pid_t pid_mostrador, pid_deposito;

#define NOMBRE_COLA "/cola_biblioteca"
#define TAM_MENSAJE 64

mqd_t cola;

sem_t sem_extraccion;
sem_t sem_registro;

#define NOMBRE_MEMORIA "/memoria_biblioteca"

int fd_memoria;
char *memoria_compartida;

int tiempo_aleatorio(int min, int max) {
    return rand() % (max - min + 1) + min;
}

void* busqueda(void* arg) {
    printf("[Hilo Búsqueda] Comienzo mi ejecución...\n");

    printf("[Hilo Búsqueda] Buscando ejemplar %s...\n", (char *)arg);
    sleep(tiempo_aleatorio(1, 3));
    printf("[Hilo Búsqueda] Ejemplar %s localizado. Enviando a extracción...\n", (char *)arg);

    sem_post(&sem_extraccion);

    return NULL;
}

void* extraccion(void* arg) {
    sem_wait(&sem_extraccion);

    printf("[Hilo Extracción] Comienzo mi ejecución...\n");

    printf("[Hilo Extracción] Extrayendo ejemplar %s de la estantería...\n", (char *)arg);
    sleep(tiempo_aleatorio(2, 4));
    printf("[Hilo Extracción] Ejemplar %s extraído. Enviando a registro...\n", (char *)arg);

    sem_post(&sem_registro);

    return NULL;
}

void* registro(void* arg) {
    sem_wait(&sem_registro);

    printf("[Hilo Registro] Comienzo mi ejecución...\n");

    printf("[Hilo Registro] Registrando préstamo del ejemplar %s...\n", (char *)arg);
    sleep(tiempo_aleatorio(1, 2));
    printf("[Hilo Registro] Préstamo del ejemplar %s registrado. Notificando al mostrador...\n", (char *)arg);

    return NULL;
}

void* recibir_notificaciones(void* arg) {

    sigset_t señales;
    siginfo_t informacion;

    sigemptyset(&señales);
    sigaddset(&señales, SIGUSR1);

    while (1) {

        // Esperar la señal del Depósito
        if (sigwaitinfo(&señales, &informacion) == SIGUSR1) {

            // Leer el identificador de la memoria compartida
            printf("[Proceso Mostrador] Recibida señal SIGUSR1.\n");

            printf("[Proceso Mostrador] Ejemplar %s preparado para entregar al usuario.\n",
                   memoria_compartida);

            // Confirmar al Depósito que se ha leído el ejemplar
            if (kill(informacion.si_pid, SIGUSR2) == -1) {
                perror("Error al enviar SIGUSR2");
            }

        } else if (errno != EINTR) {
            perror("Error al recibir SIGUSR1");
            return NULL;
        }
    }

    return NULL;
}

int main(int argc, char* argv[]) {

    struct mq_attr atributos;

    atributos.mq_flags = 0;
    atributos.mq_maxmsg = 10;
    atributos.mq_msgsize = TAM_MENSAJE;
    atributos.mq_curmsgs = 0;

    mq_unlink(NOMBRE_COLA);

    cola = mq_open(NOMBRE_COLA, O_CREAT | O_RDWR, 0666, &atributos);

    if (cola == (mqd_t)-1) {
        perror("Error al crear la cola de mensajes");
        exit(EXIT_FAILURE);
    }

    // Eliminar posibles restos de ejecuciones anteriores
    shm_unlink(NOMBRE_MEMORIA);

    // Crear la memoria compartida
    fd_memoria = shm_open(NOMBRE_MEMORIA, O_CREAT | O_RDWR, 0666);

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

    close(fd_memoria);

    // Bloquear las señales para recibirlas mediante sigwaitinfo()
    sigset_t señales;

    sigemptyset(&señales);
    sigaddset(&señales, SIGUSR1);
    sigaddset(&señales, SIGUSR2);

    if (sigprocmask(SIG_BLOCK, &señales, NULL) == -1) {
        perror("Error al bloquear las señales");

        munmap(memoria_compartida, TAM_MENSAJE);
        shm_unlink(NOMBRE_MEMORIA);
        mq_close(cola);
        mq_unlink(NOMBRE_COLA);

        exit(EXIT_FAILURE);
    }

    pid_mostrador = fork();

    if (pid_mostrador != 0) {
        pid_deposito = fork();
        if (pid_deposito != 0) {
            /* Proceso padre */

            printf("[Proceso Padre] Esperando a que terminen mis hijos...\n");

            waitpid(pid_mostrador, NULL, 0);
            waitpid(pid_deposito, NULL, 0);

            mq_close(cola);
            mq_unlink(NOMBRE_COLA);

            munmap(memoria_compartida, TAM_MENSAJE);
            shm_unlink(NOMBRE_MEMORIA);

            printf("[Proceso Padre] Ejecución finalizada.\n");
        } else {
            /* Proceso Depósito */
            printf("[Proceso Depósito] Comienzo mi ejecución...\n");

            char mensaje[TAM_MENSAJE];

            pthread_t hilo_busqueda, hilo_extraccion, hilo_registro;

            sem_init(&sem_extraccion, 0, 0);
            sem_init(&sem_registro, 0, 0);
           
            // Preparar la recepción de la confirmación del Mostrador
            sigset_t confirmacion;

            sigemptyset(&confirmacion);
            sigaddset(&confirmacion, SIGUSR2);

            while(1) {

                if (mq_receive(cola, mensaje, TAM_MENSAJE, NULL) != -1) {

                    printf("[Proceso Depósito] Recibida solicitud del ejemplar %s.\n", mensaje);

                    // Creación de los tres hilos
                    pthread_create(&hilo_busqueda, NULL, busqueda, mensaje);
                    pthread_create(&hilo_extraccion, NULL, extraccion, mensaje);
                    pthread_create(&hilo_registro, NULL, registro, mensaje);

                    // Esperamos a que finalicen los tres hilos
                    pthread_join(hilo_busqueda, NULL);
                    pthread_join(hilo_extraccion, NULL);
                    pthread_join(hilo_registro, NULL);

                    // Escribir el identificador en memoria compartida
                    strcpy(memoria_compartida, mensaje);

                    printf("[Proceso Depósito] Ejemplar %s escrito en memoria compartida.\n",
                        memoria_compartida);

                    // Notificar al Mostrador
                    printf("[Proceso Depósito] Enviando SIGUSR1 al Mostrador.\n");

                    if (kill(pid_mostrador, SIGUSR1) == -1) {
                        perror("Error al enviar SIGUSR1");
                        break;
                    }

                    // Esperar a que el Mostrador confirme la lectura
                    int resultado;

                    do {
                        resultado = sigwaitinfo(&confirmacion, NULL);
                    } while (resultado == -1 && errno == EINTR);

                    if (resultado == -1) {
                        perror("Error al recibir SIGUSR2");
                        break;
                    }

                    printf("[Proceso Depósito] Confirmación SIGUSR2 recibida.\n");

                    printf("[Proceso Depósito] Solicitud del ejemplar %s completada.\n", mensaje);

                } else {
                    perror("Error al recibir la solicitud");
                }
            }
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

        if (error != 0) {
            fprintf(stderr, "Error al crear el hilo de notificaciones: %s\n",
                    strerror(error));
            exit(EXIT_FAILURE);
        }
        
        while (1) {

            sleep(tiempo_aleatorio(3, 8));
            printf("[Proceso Mostrador] Recibida solicitud de préstamo nº %d. Enviando al depósito...\n", num_solicitud);

            snprintf(mensaje, TAM_MENSAJE, "%d", num_solicitud);

            if (mq_send(cola, mensaje, strlen(mensaje) + 1, 0) == -1) {
                perror("Error al enviar la solicitud");
            }

            num_solicitud++;

        }
    }

    exit(0);
}
