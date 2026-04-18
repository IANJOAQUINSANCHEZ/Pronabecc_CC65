package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
)

func workerLimpieza(id int, jobs <-chan []string, results chan<- []string, wg *sync.WaitGroup) {
	defer wg.Done()

	for fila := range jobs {
		// Si faltan campos clave como ID o Gestión, descartamos la fila
		if strings.TrimSpace(fila[0]) == "" || strings.TrimSpace(fila[8]) == "" {
			continue
		}

		// Completamos  campos vacíos con valores por defecto
		if strings.TrimSpace(fila[1]) == "" {
			fila[1] = "CONVOCATORIA NO ESPECIFICADA"
		}
		if strings.TrimSpace(fila[7]) == "" {
			fila[7] = "APLICA A TODAS / GENERICA"
		}

		// Mandamos fila  procesada al canal de resultados
		results <- fila
	}
}

func main() {
	tiempoInicio := time.Now()
	fmt.Printf("\nIniciando limpieza de datos...\n")

	in, _ := os.Open("Becas_1M_Limpio.csv") // Abrimos archivo de entrada
	defer in.Close()
	lector := csv.NewReader(in)
	lector.FieldsPerRecord = -1

	out, _ := os.Create("Becas_1M_Definitivo.csv") // Creamos archivo de salida
	defer out.Close()
	out.WriteString("\xEF\xBB\xBF")
	escritor := csv.NewWriter(out)

	// Lee las cabeceras y las copia al archivo final
	cabeceras, _ := lector.Read()
	escritor.Write(cabeceras)

	jobs := make(chan []string, 5000)
	results := make(chan []string, 5000)

	var wgWorkers sync.WaitGroup
	var wgWriter sync.WaitGroup

	// Este goroutine escribe todas las filas procesadas
	wgWriter.Add(1)
	go func() {
		defer wgWriter.Done()
		filasEscritas := 0
		for filaLimpia := range results {
			escritor.Write(filaLimpia)
			filasEscritas++
		}
		escritor.Flush()
		fmt.Printf("Se guardaron %d filas limpias.\n", filasEscritas)
	}()

	// Lanzamos varios workers en paralelo para procesar las filas
	numWorkers := 12
	for w := 1; w <= numWorkers; w++ {
		wgWorkers.Add(1)
		go workerLimpieza(w, jobs, results, &wgWorkers)
	}

	// Leemos el archivo y las enviamos a los workers
	for {
		fila, err := lector.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // si hay error en  fila, la saltamos
		}
		jobs <- fila
	}
	close(jobs)

	// Esperamos a que todos terminen
	wgWorkers.Wait()
	close(results)

	// Esperamos a que termine de guardar todo
	wgWriter.Wait()
	fmt.Printf("Proceso terminado en %s\n\n", time.Since(tiempoInicio))
}
