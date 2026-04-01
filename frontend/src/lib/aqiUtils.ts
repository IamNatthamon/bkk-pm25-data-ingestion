export const AQI_LEVELS = [
    { max: 15,  color: '#00E400', label: 'Good' },
    { max: 25,  color: '#FFFF00', label: 'Moderate' },
    { max: 37,  color: '#FF7E00', label: 'Unhealthy (sensitive)' },
    { max: 75,  color: '#FF0000', label: 'Unhealthy' },
    { max: 150, color: '#8F3F97', label: 'Very Unhealthy' },
    { max: Infinity, color: '#7E0023', label: 'Hazardous' },
  ]
  
  export function getAQIColor(pm25: number): string {
    return AQI_LEVELS.find(l => pm25 <= l.max)?.color ?? '#7E0023'
  }