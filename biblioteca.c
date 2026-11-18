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

pid_t pid_mostrador, pid_deposito;

int tiempo_aleatorio(int min, int max) {
    return rand() % (max - min + 1) + min;
}

void* busqueda(void* arg) {
    printf("[Hilo Búsqueda] Comienzo mi ejecución...\n");

    printf("[Hilo Búsqueda] Buscando ejemplar %s...\n", (char *)arg);
    sleep(tiempo_aleatorio(1, 3));
    printf("[Hilo Búsqueda] Ejemplar %s localizado. Enviando a extracción...\n", (char *)arg);


}

void* extraccion(void* arg) {
    printf("[Hilo Extracción] Comienzo mi ejecución...\n");

    printf("[Hilo Extracción] Extrayendo ejemplar %s de la estantería...\n", (char *)arg);
    sleep(tiempo_aleatorio(2, 4));
    printf("[Hilo Extracción] Ejemplar %s extraído. Enviando a registro...\n", (char *)arg);


}

void* registro(void* arg) {
    printf("[Hilo Registro] Comienzo mi ejecución...\n");

    printf("[Hilo Registro] Registrando préstamo del ejemplar %s...\n", (char *)arg);
    sleep(tiempo_aleatorio(1, 2));
    printf("[Hilo Registro] Préstamo del ejemplar %s registrado. Notificando al mostrador...\n", (char *)arg);

}

int main(int argc, char* argv[]) {

    pid_mostrador = fork();

    if (pid_mostrador != 0) {
        pid_deposito = fork();
        if (pid_deposito != 0) {
            /* Proceso padre */

            printf("[Proceso Padre] Esperando a que terminen mis hijos...\n");


            printf("[Proceso Padre] Ejecución finalizada.\n");
        } else {
            /* Proceso Depósito */
            printf("[Proceso Depósito] Comienzo mi ejecución...\n");

            while(1) {

            }
        }
    } else {
        /* Proceso Mostrador */
        printf("[Proceso Mostrador] Comienzo mi ejecución...\n");
        int num_solicitud = 0;

        while (1) {

            sleep(tiempo_aleatorio(3, 8));
            printf("[Proceso Mostrador] Recibida solicitud de préstamo nº %d. Enviando al depósito...\n", num_solicitud);


            num_solicitud++;

        }
    }

    exit(0);
}
