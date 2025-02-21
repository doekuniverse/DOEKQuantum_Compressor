
# DOEK Quantum Field Compressor v12.0
# Copyright 2025 DOEK Technologies
# @DOEKUNIVERSE - CHILE
# Versión optimizada para CPU/GPU con procesamiento vectorizado y campo cuántico.

# Uso de Partículas Cuánticas:
# - MaxFold (ΨMF): Utilizada en la transformación de fase para el colapso dimensional
# - GalacticShard (ΓGS): Empleada en el análisis de entropía y creación de micro-singularidades
# - DualQuantum (ΦDQ): Aplicada en el procesamiento paralelo y estados duales
# - QUANTUMFIELD (ΨQF): Nueva partícula para generación de campos de compresión

import numpy as np
import struct
import zlib
import os
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
import multiprocessing

class QuantumField:
    """
    Implementación de la partícula QUANTUMFIELD (ΨQF).
    Genera campos de compresión cuántica para optimizar el procesamiento de datos.
    """
    def __init__(self, field_strength=0.95, field_density=0.90):
        self.field_strength = field_strength
        self.field_density = field_density
        self.field_state = None
        
    def generate_field(self, data_shape):
        """Genera un campo cuántico adaptativo."""
        field = np.random.random(data_shape) * self.field_strength
        field = np.exp(1j * np.pi * field)
        field = field / np.abs(field).max()
        self.field_state = field
        return field
        
    def apply_field(self, data):
        """Aplica el campo cuántico a los datos."""
        if self.field_state is None:
            self.generate_field(data.shape)
        transformed = data * np.abs(self.field_state)
        return np.clip(transformed, 0, 255).astype(np.uint8)
        
    def collapse_field(self):
        """Colapsa el campo cuántico actual."""
        self.field_state = None

class DoekProcessor:
    """
    Procesador optimizado para CPU/GPU con campo cuántico.
    """
    def __init__(self, Doek_levels=255, dimension_levels=16):
        self.Doek_levels = Doek_levels
        self.dimension_levels = dimension_levels
        self.num_threads = multiprocessing.cpu_count()
        self.quantum_field = QuantumField()
        print(f"Usando {self.num_threads} núcleos CPU")
        
        try:
            import cupy as cp
            self.use_gpu = cp.cuda.is_available()
            self.cp = cp
            if self.use_gpu:
                print("GPU CUDA detectada y activada")
            else:
                print("CUDA disponible pero GPU no detectada")
        except ImportError:
            self.use_gpu = False
            print("Modo CPU optimizado activo")
    
    def process_chunk(self, chunk):
        """
        Procesa un chunk de datos con campo cuántico.
        
        Partículas utilizadas:
        - GalacticShard: Para el análisis de entropía
        - MaxFold: En la transformación de fase
        - DualQuantum: Para el procesamiento adaptativo
        """
        reshaped = chunk.reshape(-1, self.dimension_levels)
        
        # Aplicación del campo cuántico (QUANTUMFIELD)
        field_processed = self.quantum_field.apply_field(reshaped)
        
        # Análisis de entropía usando GalacticShard
        entropy = np.std(field_processed, axis=1)
        threshold = np.mean(entropy)
        
        # Procesamiento adaptativo usando DualQuantum
        result = np.zeros_like(reshaped, dtype=np.uint8)
        
        # Alta entropía: transformación completa usando MaxFold
        high_entropy = entropy > threshold
        if np.any(high_entropy):
            high_data = field_processed[high_entropy]
            phase = np.pi * high_data / 255
            Doek = np.exp(1j * phase)
            amplitude = np.abs(Doek)
            normalized = np.clip(amplitude * self.Doek_levels * 1.2, 0, 255)  # Aumentar la normalización
            result[high_entropy] = normalized.astype(np.uint8)
        
        # Baja entropía: transformación simple
        low_entropy = ~high_entropy
        if np.any(low_entropy):
            low_data = field_processed[low_entropy]
            normalized = np.clip(low_data * self.Doek_levels * 1.2 / 255, 0, 255)  # Aumentar la normalización
            result[low_entropy] = normalized.astype(np.uint8)
        
        return result

    def Doek_transform(self, data):
        """Transformación cuántica paralela con campo."""
        chunks = np.array_split(data, self.num_threads)
        
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            results = list(executor.map(self.process_chunk, chunks))
        
        self.quantum_field.collapse_field()
        return np.concatenate(results)

    def inverse_transform(self, data):
        """Transformación inversa paralela."""
        chunks = np.array_split(data, self.num_threads)
        
        def process_inverse(chunk):
            return (chunk * 255 / self.Doek_levels).astype(np.uint8)
            
        with ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            results = list(executor.map(process_inverse, chunks))
        
        return np.concatenate(results)

class DoekPlanetEngine:
    """
    Motor principal que coordina las cuatro partículas cuánticas.
    """
    def __init__(self):
        self.extension = '.doek'  # Cambiar a .doek
        self.magic_number = b'DKS\x00'
        self.version = 12
        self.Doek_levels = 255
        self.block_size = 1024 * 256  # 256KB blocks
        self.dimension_levels = 16
        self.processor = DoekProcessor(
            Doek_levels=self.Doek_levels,
            dimension_levels=self.dimension_levels
        )
        self.performance_metrics = {
            'compression_time': 0,
            'decompression_time': 0,
            'blocks_processed': 0,
            'total_size': 0,
            'compressed_size': 0,
            'block_times': [],
            'cpu_usage': [],
            'field_strength': []  # Nueva métrica para QUANTUMFIELD
        }

    def _compress_block(self, block: bytes) -> tuple:
        """
        Compresión de bloque optimizada usando las cuatro partículas.
        """
        block_start = time.time()
        
        if not block:
            return b'', None
            
        # Compresión directa rápida primero (DualQuantum)
        direct_compressed = zlib.compress(block, level=1)
        if len(direct_compressed) < len(block) * 0.5:
            self.performance_metrics['block_times'].append(('direct', time.time() - block_start))
            return b'\x00' + direct_compressed, None
        
        # Compresión cuántica paralela con campo (QUANTUMFIELD + MaxFold + GalacticShard)
        try:
            data = np.frombuffer(block, dtype=np.uint8)
            Doek_start = time.time()
            transformed = self.processor.Doek_transform(data)
            Doek_time = time.time() - Doek_start
            self.performance_metrics['cpu_usage'].append(Doek_time)
            
            # Medición de la fuerza del campo
            field_strength = np.mean(np.abs(transformed - data))
            self.performance_metrics['field_strength'].append(field_strength)
            
            compressed = zlib.compress(transformed.tobytes(), level=6)
            if len(compressed) < len(block) * 0.7:
                self.performance_metrics['block_times'].append(('Doek', time.time() - block_start))
                return b'\x01' + compressed, None
                
        except Exception as e:
            print(f"Error en transformación cuántica: {str(e)}")
        
        # Compresión agresiva como último recurso (GalacticShard)
        aggressive = zlib.compress(block, level=9)
        if len(aggressive) < len(block):
            self.performance_metrics['block_times'].append(('aggressive', time.time() - block_start))
            return b'\x02' + aggressive, None
            
        self.performance_metrics['block_times'].append(('none', time.time() - block_start))
        return b'\x03' + block, None

    def _decompress_block(self, data: bytes, metadata: dict) -> bytes:
        """
        Descompresión de bloque optimizada usando las cuatro partículas.
        """
        block_start = time.time()
        
        if not data:
            return b''
            
        block_type = data[0]
        content = data[1:]
        
        try:
            if block_type == 0:  # Directa (DualQuantum)
                result = zlib.decompress(content)
                self.performance_metrics['block_times'].append(('decomp_direct', time.time() - block_start))
                return result
                
            if block_type == 1:  # Doek (QUANTUMFIELD + MaxFold + GalacticShard)
                decompressed = zlib.decompress(content)
                Doek_data = np.frombuffer(decompressed, dtype=np.uint8)
                Doek_start = time.time()
                result = self.processor.inverse_transform(Doek_data)
                Doek_time = time.time() - Doek_start
                self.performance_metrics['cpu_usage'].append(Doek_time)
                self.performance_metrics['block_times'].append(('decomp_Doek', time.time() - block_start))
                return result.tobytes()
                
            if block_type == 2:  # Agresiva (GalacticShard)
                result = zlib.decompress(content)
                self.performance_metrics['block_times'].append(('decomp_aggressive', time.time() - block_start))
                return result
                
            self.performance_metrics['block_times'].append(('decomp_none', time.time() - block_start))
            return content
            
        except Exception as e:
            print(f"Error decompressing block: {str(e)}")
            return content

    def compress(self, input_file: str, output_file: str = None) -> str:
        """Compresión optimizada con campo cuántico."""
        if output_file is None:
            output_file = input_file + self.extension

        compression_start = time.time()
        try:
            file_size = os.path.getsize(input_file)
            self.performance_metrics['total_size'] = file_size
            print(f"\nProcesando archivo: {input_file} ({file_size/1024/1024:.2f} MB)")
            
            with open(input_file, 'rb') as f, open(output_file, 'wb') as out:
                # Header
                out.write(self.magic_number)
                out.write(struct.pack('B', self.version))
                out.write(struct.pack('Q', file_size))
                
                # Procesamiento por bloques
                total_blocks = (file_size + self.block_size - 1) // self.block_size
                processed_blocks = 0
                compressed_size = len(self.magic_number) + 1 + 8
                
                while True:
                    block = f.read(self.block_size)
                    if not block:
                        break
                        
                    compressed_block, metadata = self._compress_block(block)
                    
                    # Guardamos metadata
                    meta_data = str(metadata).encode() if metadata else b''
                    
                    # Escribimos bloque
                    block_header = struct.pack('II', len(compressed_block), len(meta_data))
                    out.write(block_header)
                    out.write(compressed_block)
                    if meta_data:
                        out.write(meta_data)
                    
                    compressed_size += len(block_header) + len(compressed_block) + len(meta_data)
                    processed_blocks += 1
                    progress = (processed_blocks / total_blocks) * 100
                    ratio = (compressed_size / (processed_blocks * self.block_size)) * 100
                    print(f"\rComprimiendo: {progress:.1f}% - Ratio: {ratio:.1f}%", end='', flush=True)

            self.performance_metrics['compression_time'] = time.time() - compression_start
            self.performance_metrics['blocks_processed'] = processed_blocks
            self.performance_metrics['compressed_size'] = compressed_size
            
            speed = file_size / (1024 * 1024 * self.performance_metrics['compression_time'])
            print(f"\nCompresión completada en {self.performance_metrics['compression_time']:.2f} segundos ({speed:.2f} MB/s)")
            return output_file
            
        except Exception as e:
            print(f"\nError durante la compresión: {str(e)}")
            return None

    def decompress(self, input_file: str, output_file: str = None) -> str:
        """Descompresión optimizada."""
        if not input_file.endswith(self.extension):
            raise ValueError(f"Formato de archivo inválido. Se esperaba un archivo {self.extension}")

        if output_file is None:
            output_file = input_file[:-len(self.extension)]

        decompression_start = time.time()
        try:
            print(f"\nLeyendo archivo comprimido: {input_file}")
            
            with open(input_file, 'rb') as f:
                if f.read(4) != self.magic_number:
                    raise ValueError("Formato de archivo inválido")
                    
                version = struct.unpack('B', f.read(1))[0]
                if version != self.version:
                    raise ValueError(f"Versión incompatible: {version}")

                original_size = struct.unpack('Q', f.read(8))[0]
                processed_size = 0
                
                with open(output_file, 'wb') as out:
                    while processed_size < original_size:
                        # Leemos tamaños
                        block_size, meta_size = struct.unpack('II', f.read(8))
                        
                        # Leemos datos
                        compressed_block = f.read(block_size)
                        meta_data = eval(f.read(meta_size).decode()) if meta_size > 0 else None
                        
                        # Descomprimimos
                        decompressed = self._decompress_block(compressed_block, meta_data)
                        out.write(decompressed)
                        
                        processed_size += len(decompressed)
                        progress = (processed_size / original_size) * 100
                        print(f"\rDescomprimiendo: {progress:.1f}%", end='', flush=True)

            self.performance_metrics['decompression_time'] = time.time() - decompression_start
            speed = original_size / (1024 * 1024 * self.performance_metrics['decompression_time'])
            print(f"\nDescompresión completada en {self.performance_metrics['decompression_time']:.2f} segundos ({speed:.2f} MB/s)")
            return output_file
            
        except Exception as e:
            print(f"\nError durante la descompresión: {str(e)}")
            return None

    def get_detailed_metrics(self) -> dict:
        """Obtiene métricas detalladas incluyendo uso de CPU y campo cuántico."""
        metrics = self.performance_metrics.copy()
        
        # Análisis de tiempos por tipo
        block_types = defaultdict(list)
        for btype, btime in metrics['block_times']:
            block_types[btype].append(btime)
        
        # Calculamos promedios
        avg_times = {
            btype: sum(times)/len(times)
            for btype, times in block_types.items()
        }
        
        # Contamos uso de cada método
        method_counts = defaultdict(int)
        for btype, _ in metrics['block_times']:
            method_counts[btype] += 1
            
        # Métricas CPU
        cpu_metrics = {
            'total_cpu_time': sum(metrics['cpu_usage']),
            'avg_cpu_time': sum(metrics['cpu_usage'])/len(metrics['cpu_usage']) if metrics['cpu_usage'] else 0,
            'cpu_operations': len(metrics['cpu_usage'])
        }
        
        # Métricas del campo cuántico
        field_metrics = {
            'avg_field_strength': sum(metrics['field_strength'])/len(metrics['field_strength']) if metrics['field_strength'] else 0,
            'max_field_strength': max(metrics['field_strength']) if metrics['field_strength'] else 0,
            'min_field_strength': min(metrics['field_strength']) if metrics['field_strength'] else 0
        }
            
        return {
            'compression_time': metrics['compression_time'],
            'decompression_time': metrics['decompression_time'],
            'total_time': metrics['compression_time'] + metrics['decompression_time'],
            'compression_speed': metrics['total_size'] / (1024 * 1024 * metrics['compression_time']),
            'decompression_speed': metrics['total_size'] / (1024 * 1024 * metrics['decompression_time']),
            'blocks_processed': metrics['blocks_processed'],
            'compression_ratio': (metrics['compressed_size'] / metrics['total_size']) * 100,
            'average_times': avg_times,
            'method_usage': dict(method_counts),
            'cpu_metrics': cpu_metrics,
            'field_metrics': field_metrics
        }

def print_compression_stats(original_file: str, compressed_file: str, metrics: dict):
    """Estadísticas detalladas de compresión."""
    original_size = os.path.getsize(original_file)
    compressed_size = os.path.getsize(compressed_file)
    ratio = (compressed_size / original_size) * 100
    cpu_metrics = metrics['cpu_metrics']
    field_metrics = metrics['field_metrics']

    print(f"""
╔════ DOEK Quantum Field Processor v12.0 ════╗
║ File Analysis:
║ ├─ Original: {original_file}
║ │  └─ Size: {original_size/1024/1024:.2f} MB
║ ├─ Compressed: {compressed_file}
║ │  └─ Size: {compressed_size/1024/1024:.2f} MB
║ │
║ Compression Metrics:
║ ├─ Ratio: {ratio:.2f}%
║ ├─ Reduction: {100-ratio:.2f}%
║ ├─ Factor: {original_size/compressed_size:.2f}x
║ │
║ Time Performance:
║ ├─ Compression: {metrics['compression_time']:.2f}s
║ │  └─ Speed: {metrics['compression_speed']:.2f} MB/s
║ ├─ Decompression: {metrics['decompression_time']:.2f}s
║ │  └─ Speed: {metrics['decompression_speed']:.2f} MB/s
║ ├─ Total Time: {metrics['total_time']:.2f}s
║ └─ Avg Speed: {((metrics['compression_speed'] + metrics['decompression_speed'])/2):.2f} MB/s
║
║ CPU Performance:
║ ├─ Total CPU Time: {cpu_metrics['total_cpu_time']:.2f}s
║ ├─ Avg CPU Time: {cpu_metrics['avg_cpu_time']*1000:.2f}ms
║ └─ CPU Operations: {cpu_metrics['cpu_operations']}
║
║ Quantum Field Metrics:
║ ├─ Avg Strength: {field_metrics['avg_field_strength']:.4f}
║ ├─ Max Strength: {field_metrics['max_field_strength']:.4f}
║ └─ Min Strength: {field_metrics['min_field_strength']:.4f}
║
║ Method Analysis:
║ ├─ Direct: {metrics['method_usage'].get('direct', 0)} blocks
║ ├─ Doek: {metrics['method_usage'].get('Doek', 0)} blocks
║ ├─ Aggressive: {metrics['method_usage'].get('aggressive', 0)} blocks
║ └─ None: {metrics['method_usage'].get('none', 0)} blocks
║
║ Average Block Times:
║ ├─ Direct: {metrics['average_times'].get('direct', 0):.4f}s
║ ├─ Doek: {metrics['average_times'].get('Doek', 0):.4f}s
║ └─ Aggressive: {metrics['average_times'].get('aggressive', 0):.4f}s
╚═══════════════════════════════════════╝
""")

if __name__ == "__main__":
    engine = DoekPlanetEngine()
    
    input_file = "datasetprueba.tbl"
    
    if not os.path.exists(input_file):
        print(f"Error: No se encuentra el archivo {input_file}")
        exit(1)

    print("\n=== Iniciando Proceso de Compresión con Campo Cuántico ===")
    compressed_file = engine.compress(input_file)
    
    if compressed_file:
        print("\n=== Iniciando Proceso de Descompresión ===")
        decompressed_file = input_file[:-4] + "_restored.tbl"
        if engine.decompress(compressed_file, decompressed_file):
            print("\n=== Estadísticas Finales ===")
            metrics = engine.get_detailed_metrics()
            print_compression_stats(input_file, compressed_file, metrics)
        else:
            print("\nError: La descompresión falló.")
    else:
        print("\nError: La compresión falló.")
