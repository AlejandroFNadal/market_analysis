import styles from './page.module.css'
import TimeSeriesChart from './components/TimeSeries'
import localFont from 'next/font/local'

const alienWorld = localFont({ src :'./fonts/AlienWorld.ttf'})

export default function Home() {
  return (
    <main className={styles.main}>
      <div className={alienWorld.className}>
        <h1 className={styles.title}>
          ETHEREUM GAS TRACKER
        </h1>
      </div>
        <p className={styles.description}>
          In here, you can see the gas price for Ethereum in USD, with a hour granularity. The data is updated on real time, every 2 minutes.
        </p>


      <div className={styles.center}>
        <TimeSeriesChart />
     </div>
      <div className={styles.footer}>
        <p>
          Made with ❤️ by <a href="alejandronadal.com">Alejandro Nadal</a>. Powered by AWS, Kafka, Airflow, and Ethereum.
        </p>
      </div>
    </main>
  )
}
