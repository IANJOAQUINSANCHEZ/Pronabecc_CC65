package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type TareaETL struct {
	Nombre          string
	ArchivoIn       string
	ArchivoOut      string
	Multiplicador   int
	NumWorkers      int
	EsEstudiante    bool
	NuevasCabeceras []string
}

func worker(id int, tarea TareaETL, jobs <-chan []string, results chan<- [][]string, wg *sync.WaitGroup) {
	defer wg.Done()

	for filaOriginal := range jobs {
		var bloqueMultiplicado [][]string

		for i := 0; i < tarea.Multiplicador; i++ {
			filaCopia := make([]string, len(filaOriginal))
			copy(filaCopia, filaOriginal)

			if tarea.EsEstudiante {
				// Generamos IDs únicos para cada fila de estudiante
				if i > 0 {
					filaCopia[2] = fmt.Sprintf("%s-V%d", filaCopia[2], i)
				}
				bloqueMultiplicado = append(bloqueMultiplicado, filaCopia)
			} else {
				// Nos quedamos solo con las columnas que nos interesan para las becas
				filaLimpia := []string{
					filaCopia[2],
					filaCopia[3],
					filaCopia[4],
					filaCopia[5],
					filaCopia[6],
					filaCopia[7],
					filaCopia[8],
					filaCopia[9],
					filaCopia[11],
				}
				bloqueMultiplicado = append(bloqueMultiplicado, filaLimpia)
			}
		}

		results <- bloqueMultiplicado
	}
}

func ejecutarPipeline(tarea TareaETL) {
	fmt.Printf("\n Iniciando Pipeline: %s\n", tarea.Nombre)

	in, _ := os.Open(tarea.ArchivoIn) // Abrimos el archivo de entrada
	defer in.Close()

	lector := csv.NewReader(in)
	lector.FieldsPerRecord = -1 // Permitimos filas con diferente número de columnas

	out, _ := os.Create(tarea.ArchivoOut) // Creamos el archivo de salida
	defer out.Close()

	// BOM para que abra bien el archivo Excel
	out.WriteString("\xEF\xBB\xBF")
	escritor := csv.NewWriter(out)

	// Si hay nuevas cabeceras, las escribimos primero
	if len(tarea.NuevasCabeceras) > 0 {
		escritor.Write(tarea.NuevasCabeceras)
	}

	jobs := make(chan []string, 2000)
	results := make(chan [][]string, 2000)

	var wgWorkers sync.WaitGroup
	var wgWriter sync.WaitGroup

	// Goroutine que escribe los resultados en el archivo
	wgWriter.Add(1)
	go func() {
		defer wgWriter.Done()
		for bloque := range results {
			for _, fila := range bloque {
				escritor.Write(fila)
			}
		}
		escritor.Flush()
	}()

	// Lanzamos  workers que procesan filas en paralelo
	for w := 1; w <= tarea.NumWorkers; w++ {
		wgWorkers.Add(1)
		go worker(w, tarea, jobs, results, &wgWorkers)
	}

	filasLeidas := 0
	for {
		fila, err := lector.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue // Si hay error en una fila, la saltamos
		}

		if filasLeidas == 0 {
			filasLeidas++
			continue // Saltamos la cabecera original
		}

		jobs <- fila
		filasLeidas++
	}
	close(jobs)

	wgWorkers.Wait()
	close(results)

	wgWriter.Wait()
	fmt.Printf("%s finalizado.\n", tarea.Nombre)
}

func main() {
	tiempoInicio := time.Now()

	tareaBecas := TareaETL{
		Nombre:        "Aumento de Catálogo de Becas",
		ArchivoIn:     "datos_raw/becas/ConvocatoriaPorCarreraSede_Top200.csv", // Ruta del archivo original
		ArchivoOut:    "Becas_1M_Limpio.csv",                                   // Archivo generado
		Multiplicador: 20,
		NumWorkers:    8,
		EsEstudiante:  false,
		NuevasCabeceras: []string{
			"ID_Convocatoria", "Nombre_Convocatoria", "Pais",
			"Nivel", "Tipo_Institucion", "Sede", "Institucion", "Carrera", "Tipo_Gestion",
		},
	}

	tareaEstudiantes := TareaETL{
		Nombre:        "Simulación de 100k Postulantes",
		ArchivoIn:     "datos_raw/personas/Credito18DatosSolicitante.csv", // Datos base de personas
		ArchivoOut:    "Estudiantes_100k_Limpio.csv",                      // Resultado simulado
		Multiplicador: 50,
		NumWorkers:    4,
		EsEstudiante:  true,
		NuevasCabeceras: []string{
			"ID_Fila", "ID_Original", "ID_Postulante", "Convocatoria",
			"Tipo_Estudiante", "Tipo_Familiar", "Tiene_Telf_Fijo",
			"Tiene_Celular", "Tiene_Email", "Ingresos_Mensuales",
			"Gastos_Mensuales", "Fecha_Registro", "Ficha_Socioeconomica",
			"Genero", "Distrito", "Fecha_Hora",
		},
	}

	// Ejecutamos procesos
	ejecutarPipeline(tareaBecas)
	ejecutarPipeline(tareaEstudiantes)

	fmt.Printf("\nETL FINALIZADO en %s \n", time.Since(tiempoInicio))
}
