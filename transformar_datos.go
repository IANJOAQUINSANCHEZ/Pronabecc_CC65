package main

import (
	"encoding/csv"
	"fmt"
	"hash/fnv"
	"io"
	"os"
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

func main() {
	inputPath := "Estudiantes_100k_Limpio.csv"
	outputPath := "Estudiantes_Final.csv"

	file, _ := os.Open(inputPath)
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

		idUnico := fila[2] + fila[3]
		h := getHash(idUnico)

		nivel := niveles[h%uint32(len(niveles))]
		gestion := gestiones[(h/uint32(len(niveles)))%uint32(len(gestiones))]

		fila[4] = fmt.Sprintf("%s|%s", nivel, gestion)

		writer.Write(fila)
		count++
	}
	fmt.Printf("¡Listo! Se han transformado %d registros.\n", count)
}
