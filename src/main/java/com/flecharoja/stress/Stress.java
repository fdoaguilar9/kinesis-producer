package com.flecharoja.stress;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import java.nio.ByteBuffer;
import com.amazonaws.services.kinesis.producer.KinesisProducerConfiguration;
import com.amazonaws.services.kinesis.producer.KinesisProducer;
import com.amazonaws.services.kinesis.producer.UserRecordResult;
import com.google.common.util.concurrent.ListenableFuture;

public class Stress {
    private static final int CANTIDAD_ITERACIONES = 100;
    private static final int CANTIDAD_THREADS = 4;
    private static final String STREAM_NAME = "PrototipoMH";
    private static final String REGION = "us-east-1";
    private static final Logger LOG = Logger.getLogger(Stress.class.getName());

    public static void main(String args[]) {
        System.out.println("---------------------------------");
        System.out.println("AWS Kinesis DataStreams Stress Tester v 1.0");
        System.out.println("---------------------------------");

        // Buscamos aquellos que estan pendientes de ser procesados
        ExecutorService executor = Executors.newFixedThreadPool(CANTIDAD_THREADS);

        for (int i = 1; i <= CANTIDAD_THREADS; i++) {
            Runnable worker = new MyRunnable(i);
            executor.execute(worker);
        }
        executor.shutdown();
        // Espera a que los threads hayan terminado
        while (!executor.isTerminated()) {

        }
        System.exit(0);
    }

    public static class MyRunnable implements Runnable {
        // Cliente de Kinesis para producir mensajes
        final KinesisProducer producer;
        //Numero de secuencia unico por iteracion por thread
        static final AtomicLong sequenceNumber = new AtomicLong(0);
        // Random usado para generar id de mensaje
        private static final Random RANDOM = new Random();
        // Numero de thread obtenido del proceso principal
        private final int proceso;

        MyRunnable(int proc) {
            this.proceso = proc;
            // Obtenemos el producer
            this.producer = getKinesisProducer(REGION);
        }

        /**
         * Encargado de generar un explicit hash key, esto para particionar los mensajes por shard
         * En este caso estamos usando una estrategia random para tratar de balancear la carga entre todos los shards
         * @return Un entero random convertido a string.
         */
        public static String randomExplicitHashKey() {
            return new BigInteger(128, RANDOM).toString(10);
        }

        /**
         * Encargado de generar el body de la peticion
         * En este caso solamente incrementamos el consecutivo global y lo ponemos junto a un Hello World
         * @return
         */
        public static ByteBuffer generateData() {
            StringBuilder sb = new StringBuilder();
            //se constryue un JSON usando el numero de secuencia unico
            sb.append("{ \"message\": \"Hello World!\", \"consec\": "+ sequenceNumber.getAndIncrement() +"}");
            try {
                //retornamos un Bytebuffer con el string codificado como utf-8
                return ByteBuffer.wrap(sb.toString().getBytes("UTF-8"));
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void run() {
            Long inicio = Calendar.getInstance().getTimeInMillis();
            // Iteramos segun la config incial
            for (int i = 0; i < CANTIDAD_ITERACIONES; i++) {
                System.out.println("\tProcesando Thread (" + proceso + ") Iteracion # " + i);
                // Obtenemos la data del metodo generador
                ByteBuffer data = generateData();
                // Envia el mensaje a Kinesis, el resultado se almacena en un Future, el cual en el momento de respuesta reflejara si hubo error o success
                ListenableFuture<UserRecordResult> f = producer.addUserRecord(STREAM_NAME, Long.toString(System.currentTimeMillis()), randomExplicitHashKey(), data);
                // Espera a la respuesta de Kinesis
                while (!f.isDone()) {
                    try {
                        // Si no esta listo espera 100ms
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                try {
                    // Intentamos obtener el resultado, en este caso imprimo si fue exitoso
                    System.out.println("\tResultado de invocacion, successful = " +f.get().isSuccessful()); ;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            Long fin = Calendar.getInstance().getTimeInMillis();
            System.out.println("Fin Thread (" + proceso + ") Tardo: " + (fin - inicio) + " ms");
        }

        /**
         * @param region La region a usar  con el producer
         * @return KinesisProducer instancia usada para poner los records en el stream.
         */
        public static KinesisProducer getKinesisProducer(final String region) {
            KinesisProducerConfiguration config = new KinesisProducerConfiguration();

            // Seteamos la region, dado que queremos usar la misma region en la que esta la instancia de ec2
            config.setRegion(region);

            // Es posible pasar credenciales programaticamente a traves de la config
            config.setCredentialsProvider(new DefaultAWSCredentialsProviderChain());

            // Controla el nivel de paralelismo usando conexiones HTTP. Si se establece muy alto podria causar errores
            // de broken pipe dado que habrian conexiones sin usar, y errores de request timeouts si no hay suficiente ancho de banda
            config.setMaxConnections(1);

            // Controlamos el timeout, incrementar en conexiones lentas
            config.setRequestTimeout(60000);

            // Controla que tanto tiempo los records se les permite quedarse en espera en el buffer antes de ser enviados.
            // config.setRecordMaxBufferedTime(2000);

            return new KinesisProducer(config);
        }
    }

}
