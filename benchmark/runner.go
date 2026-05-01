package main

import (
	"encoding/csv"
	"fmt"
	"math"
	"os"
	"os/exec"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	NumEjecuciones = 10
	RecorteSup     = 1
	RecorteInf     = 1
	MuestrasUtiles = NumEjecuciones - RecorteSup - RecorteInf
)

var nivelesWorkers = []int{2, 4, 8, 12, 16}

// RESTRUCTURA PARA GUARDAR BENCHMARKS
type ResultadoBench struct {
	Modo           string
	Workers        int
	TiemposRaw     []float64
	MediaRecortada float64
	Speedup        float64
	Throughput     float64
	Eficiencia     float64
}

func main() {
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║  BENCHMARK: Motor de Recomendaciones PRONABEC          ║")
	fmt.Printf("║  CPU: %d cores lógicos | OS: %s/%s               ║\n", runtime.NumCPU(), runtime.GOOS, runtime.GOARCH)
	fmt.Println("║  Protocolo: Media recortada (10 runs, descarta min/max)║")
	fmt.Println("╚══════════════════════════════════════════════════════════╝")

	// COMPILAR EL PROGRAMA PARA EJECUCIÓN MEDIANTE ESTE ARCHIVO
	fmt.Println("\n[1/3] Compilando programas...")
	compilar("cmd/main_sec.go", "main_sec.exe")
	compilar("cmd/main_con.go", "main_con.exe")

	var resultados []ResultadoBench

	// BENCHMARK SECUENCIAL
	fmt.Printf("\n[2/3] Ejecutando versión SECUENCIAL (%d runs)...\n", NumEjecuciones)
	tiemposSec := ejecutarNVeces("main_sec.exe", "", NumEjecuciones)
	mediaSec := mediaRecortada(tiemposSec)

	resultados = append(resultados, ResultadoBench{
		Modo:           "SECUENCIAL",
		Workers:        1,
		TiemposRaw:     tiemposSec,
		MediaRecortada: mediaSec,
		Speedup:        1.0,
		Throughput:     100000.0 / mediaSec,
		Eficiencia:     1.0,
	})

	// BENCHMARK CONCURRENTE
	fmt.Printf("\n[3/3] Ejecutando versión CONCURRENTE (configs: %v)...\n", nivelesWorkers)
	for _, nw := range nivelesWorkers {
		fmt.Printf("\n  → Workers = %d (%d runs)...\n", nw, NumEjecuciones)
		args := fmt.Sprintf("-workers=%d", nw)
		tiempos := ejecutarNVeces("main_con.exe", args, NumEjecuciones)
		media := mediaRecortada(tiempos)

		speedup := mediaSec / media
		resultados = append(resultados, ResultadoBench{
			Modo:           fmt.Sprintf("CONCURRENTE-%dw", nw),
			Workers:        nw,
			TiemposRaw:     tiempos,
			MediaRecortada: media,
			Speedup:        speedup,
			Throughput:     100000.0 / media,
			Eficiencia:     speedup / float64(nw),
		})
	}

	// IMPRIMIR RESULTADOS
	imprimirTabla(resultados)
	guardarCSV(resultados)

	fmt.Println("\n✅ Benchmark completo. Resultados guardados en benchmark_results.csv")
}

// COMPILAR EL PROGRAMA
func compilar(fuente, binario string) {
	fmt.Printf("  Compilando %s → %s...", fuente, binario)
	cmd := exec.Command("go", "build", "-o", binario, fuente)
	output, err := cmd.CombinedOutput()
	if err != nil {
		fmt.Printf(" ERROR\n%s\n", string(output))
		os.Exit(1)
	}
	fmt.Println(" OK")
}

// EJECUTAR EL PROGRAMA N VECES
func ejecutarNVeces(binario, args string, n int) []float64 {
	tiempos := make([]float64, n)

	for i := 0; i < n; i++ {
		inicio := time.Now()

		var cmd *exec.Cmd
		if args != "" {
			cmd = exec.Command("./"+binario, strings.Fields(args)...)
		} else {
			cmd = exec.Command("./" + binario)
		}

		cmd.Stdout = nil
		cmd.Stderr = nil

		err := cmd.Run()
		duracion := time.Since(inicio).Seconds()

		if err != nil {
			fmt.Printf("    Run %d: ERROR (%v)\n", i+1, err)
			tiempos[i] = math.MaxFloat64
		} else {
			tiempos[i] = duracion
			fmt.Printf("    Run %d/%d: %.3fs\n", i+1, n, duracion)
		}
	}

	return tiempos
}

func mediaRecortada(tiempos []float64) float64 {
	sorted := make([]float64, len(tiempos))
	copy(sorted, tiempos)
	sort.Float64s(sorted)

	// ELIMINAR EL MEJOR Y EL PEOR
	recortados := sorted[RecorteInf : len(sorted)-RecorteSup]

	suma := 0.0
	for _, t := range recortados {
		suma += t
	}
	return suma / float64(len(recortados))
}

// FORMATO DE LA TABLA
func imprimirTabla(resultados []ResultadoBench) {
	fmt.Println("\n╔══════════════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                     RESULTADOS DEL BENCHMARK                            ║")
	fmt.Println("╠═══════════════════╦══════════╦══════════╦═══════════╦════════╦═══════════╣")
	fmt.Println("║ Configuración     ║ Workers  ║ T(s)     ║ Speedup   ║ Eff.   ║ Throughput║")
	fmt.Println("╠═══════════════════╬══════════╬══════════╬═══════════╬════════╬═══════════╣")

	for _, r := range resultados {
		fmt.Printf("║ %-17s ║ %8d ║ %8.3f ║ %9.2fx ║ %5.1f%% ║ %7.0f/s ║\n",
			r.Modo, r.Workers, r.MediaRecortada, r.Speedup, r.Eficiencia*100, r.Throughput)
	}

	fmt.Println("╚═══════════════════╩══════════╩══════════╩═══════════╩════════╩═══════════╝")

	// ANÁLISIS AUTOMÁTICO DEL PUNTO DE SATURACIÓN
	mejorSpeedup := 0.0
	mejorWorkers := 0
	for _, r := range resultados {
		if r.Speedup > mejorSpeedup {
			mejorSpeedup = r.Speedup
			mejorWorkers = r.Workers
		}
	}
	fmt.Printf("\n📊 Mejor Speedup: %.2fx con %d workers\n", mejorSpeedup, mejorWorkers)
	fmt.Printf("📊 CPUs disponibles: %d lógicos\n", runtime.NumCPU())

	// DETECTAR RETORNO DISMINUIDO
	for i := 2; i < len(resultados); i++ {
		if resultados[i].Speedup < resultados[i-1].Speedup {
			fmt.Printf("⚠️  Punto de retorno disminuido detectado en %d workers\n", resultados[i].Workers)
			break
		}
	}
}

// EXPORTAR RESULTADOS A CSV
func guardarCSV(resultados []ResultadoBench) {
	file, err := os.Create("benchmark_results.csv")
	if err != nil {
		fmt.Printf("Error creando CSV: %v\n", err)
		return
	}
	defer file.Close()

	file.WriteString("\xEF\xBB\xBF")
	writer := csv.NewWriter(file)
	defer writer.Flush()

	// FORMATO PARA LAS COLUMNAS
	header := []string{"Configuracion", "Workers", "Media_Recortada_s", "Speedup", "Eficiencia", "Throughput_est_s"}
	for i := 1; i <= NumEjecuciones; i++ {
		header = append(header, fmt.Sprintf("Run_%d_s", i))
	}
	writer.Write(header)

	for _, r := range resultados {
		row := []string{
			r.Modo,
			strconv.Itoa(r.Workers),
			fmt.Sprintf("%.3f", r.MediaRecortada),
			fmt.Sprintf("%.2f", r.Speedup),
			fmt.Sprintf("%.4f", r.Eficiencia),
			fmt.Sprintf("%.0f", r.Throughput),
		}
		for _, t := range r.TiemposRaw {
			row = append(row, fmt.Sprintf("%.3f", t))
		}
		writer.Write(row)
	}
}
