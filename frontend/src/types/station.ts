export interface Station {
    id: string
    station_code: string
    name: string
    lat: number
    lon: number
    region: string
    province: string
  }
  
  export interface PM25Reading {
    id: string
    station_id: string
    measured_at: string
    pm2_5: number
    pm10: number
    aqi: number
  }