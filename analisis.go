package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

func main() {
	tiempoInicio := time.Now()
	archivoInput := "Becas_1M_Limpio.csv"

	fmt.Printf("\n🔍 Iniciando Análisis Concurrente de Nulos en: %s\n", archivoInput)

	in, err := os.Open(archivoInput)
	if err != nil {
		fmt.Printf("❌ Error al abrir %s: %v\n", archivoInput, err)
		return
	}
	defer in.Close()

	lector := csv.NewReader(in)
	lector.FieldsPerRecord = -1

	// Leer la primera fila para obtener los nombres de las columnas
	cabeceras, err := lector.Read()
	if err != nil {
		fmt.Printf("❌ Error al leer cabeceras: %v\n", err)
		return
	}

	// Variables compartidas (Estado Global)
	numColumnas := len(cabeceras)
	totalNulosGlobal := make([]int, numColumnas)
	totalFilasGlobal := 0
	var mu sync.Mutex

	// Canales y Sincronización
	jobs := make(chan []string, 5000) // Buffer grande para 1.2M de datos
	var wg sync.WaitGroup
	numWorkers := 12

	// --- INICIAR WORKERS ---
	for w := 1; w <= numWorkers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			// 1. Estado Local (Para evitar pelear por el Mutex en cada fila)
			nulosLocales := make([]int, numColumnas)
			filasLocales := 0

			// 2. Procesamiento (Map)
			for fila := range jobs {
				filasLocales++
				// Solo analizamos hasta donde tenga columnas la fila para evitar "index out of range"
				limite := len(fila)
				if limite > numColumnas {
					limite = numColumnas
				}

				for i := 0; i < limite; i++ {
					// En CSV, un nulo suele ser un string vacío o solo espacios
					if fila[i] == "" || fila[i] == " " {
						nulosLocales[i]++
					}
				}
			}

			// 3. Volcado final al Estado Global (Reduce)
			// SECCIÓN CRÍTICA: Aquí usamos el candado de forma eficiente
			mu.Lock()
			totalFilasGlobal += filasLocales
			for i := 0; i < numColumnas; i++ {
				totalNulosGlobal[i] += nulosLocales[i]
			}
			mu.Unlock()
		}()
	}

	// --- LECTURA DEL ARCHIVO (Productor) ---
	for {
		fila, err := lector.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}
		jobs <- fila
	}
	close(jobs) // Avisamos a los workers que ya no hay más filas

	// Esperamos a que todos terminen de contar
	wg.Wait()

	// --- IMPRIMIR RESULTADOS ---
	fmt.Printf("\n📊 REPORTE DE CALIDAD DE DATOS 📊\n")
	fmt.Printf("==================================================\n")
	fmt.Printf("Total de registros analizados: %d\n", totalFilasGlobal)
	fmt.Printf("==================================================\n")

	hayNulos := false
	for i := 0; i < numColumnas; i++ {
		porcentaje := (float64(totalNulosGlobal[i]) / float64(totalFilasGlobal)) * 100
		if totalNulosGlobal[i] > 0 {
			hayNulos = true
			fmt.Printf("⚠️  Columna '%s': %d nulos (%.2f%%)\n", cabeceras[i], totalNulosGlobal[i], porcentaje)
		} else {
			fmt.Printf("✅  Columna '%s': 0 nulos (Impecable)\n", cabeceras[i])
		}
	}

	if !hayNulos {
		fmt.Printf("\n✨ ¡Tu dataset está perfectamente limpio! ✨\n")
	}

	fmt.Printf("==================================================\n")
	fmt.Printf("⏱️ Tiempo de análisis: %s\n\n", time.Since(tiempoInicio))
}
