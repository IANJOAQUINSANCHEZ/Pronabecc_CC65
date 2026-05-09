package main

import (
	"encoding/csv"
	"fmt"
	"hash/fnv"
	"io"
	"math/rand"
	"os"
	"strings"
	"time"
)

var niveles = []string{
	"MAESTRIA", "CARRERA UNIVERSITARIA", "DOCTORADO", "CARRERA TÉCNICA",
	"IDIOMAS", "DIPLOMADO", "CURSO LIBRE", "PRE-UNIVERSITARIO",
	"CURSO DE EXTENSIÓN", "TECNOLÓGICA - PEDAGÓGICA", "TECNICO-PRODUCTIVA",
}

var gestiones = []string{"PÚBLICA", "PRIVADA"}

func getHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// CARGA LOS NOMBRES ÚNICOS DE LAS BECAS PARA SIMULAR CONVOCATORIAS
func cargarNombresBecas(ruta string) ([]string, error) {
	file, err := os.Open(ruta)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	lector := csv.NewReader(file)
	lector.FieldsPerRecord = -1
	lector.LazyQuotes = true

	// Saltamos cabecera
	lector.Read()

	nombresMap := make(map[string]bool)
	for {
		fila, err := lector.Read()
		if err == io.EOF {
			break
		}
		if len(fila) > 0 && fila[0] != "" {
			nombresMap[strings.ToUpper(strings.TrimSpace(fila[0]))] = true
		}
	}

	nombres := make([]string, 0, len(nombresMap))
	for n := range nombresMap {
		nombres = append(nombres, n)
	}
	return nombres, nil
}

func main() {
	// Usamos una semilla fija para reproducibilidad o basada en tiempo
	rand.Seed(time.Now().UnixNano())

	becasPath := "Becas_1M_Definitivo.csv"
	inputPath := "Estudiantes_100k_Limpio.csv"
	outputPath := "Estudiantes_Final.csv"

	fmt.Println("Cargando nombres de becas para inyección...")
	nombresBecas, err := cargarNombresBecas(becasPath)
	if err != nil {
		fmt.Printf("Error cargando becas: %v\n", err)
		return
	}
	fmt.Printf("Se cargaron %d nombres de becas únicos.\n", len(nombresBecas))

	file, err := os.Open(inputPath)
	if err != nil {
		fmt.Printf("Error abriendo estudiantes: %v\n", err)
		return
	}
	defer file.Close()

	outFile, _ := os.Create(outputPath)
	defer outFile.Close()

	reader := csv.NewReader(file)
	writer := csv.NewWriter(outFile)
	defer writer.Flush()

	header, _ := reader.Read()
	writer.Write(header)

	count := 0
	for {
		fila, err := reader.Read()
		if err == io.EOF {
			break
		}

		if len(fila) < 5 {
			continue
		}

		// 1. INYECTAR CONVOCATORIA ALEATORIA (Columna 3)
		// Elegimos una beca al azar del catálogo real
		fila[3] = nombresBecas[rand.Intn(len(nombresBecas))]

		// 2. GENERAR CLAVE COMPUESTA (Columna 4)
		idUnico := fila[2]
		h := getHash(idUnico)
		nivel := niveles[h%uint32(len(niveles))]
		gestion := gestiones[(h/uint32(len(niveles)))%uint32(len(gestiones))]
		fila[4] = fmt.Sprintf("%s|%s", nivel, gestion)

		writer.Write(fila)
		count++
	}
	fmt.Printf("¡Éxito! Se han transformado %d registros en %s.\n", count, outputPath)
}
