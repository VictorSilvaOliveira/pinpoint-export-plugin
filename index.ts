import { createBuffer } from '@posthog/plugin-contrib'
import { Event, EventsBatch, PinpointClient, PutEventsCommand } from '@aws-sdk/client-pinpoint'
import { Plugin, PluginMeta, PluginEvent } from '@posthog/plugin-scaffold'

type PintpointPlugin = Plugin<{
    global: {
        pinpoint: PinpointClient
        buffer: ReturnType<typeof createBuffer>
        eventsToIgnore: Set<string>
    }
    config: {
        awsAccessKey: string
        awsSecretAccessKey: string
        awsRegion: string
        applicationId: string
        uploadSeconds: string
        uploadMegabytes: string
        eventsToIgnore: string,
        maxAttempts: number
    }
    jobs: {}
}>

// export const jobs: PintpointPlugin['jobs'] = {
//     putBatchEvents: async (payload, meta) => {
//         await sendEventsToPinpoint(payload, meta)
//     },
// }

// export const sendEventsToPinpoint = async (payload: UploadJobPayload, meta: PluginMeta<PintpointPlugin>) => {
//     const { global, config, jobs } = meta

//     const { batch } = payload
//     const date = new Date().toISOString()
//     const [day, time] = date.split('T')
//     const dayTime = `${day.split('-').join('')}-${time.split(':').join('')}`
//     const suffix = randomBytes(8).toString('hex')
//     console.log(`Flushing ${batch.length} events!`)

//     global.pinpoint.send()
// }

export const setupPlugin: PintpointPlugin['setupPlugin'] = (meta) => {
    const { global, config, jobs } = meta
    if (!config.awsAccessKey) {
        throw new Error('AWS access key missing!')
    }
    if (!config.awsSecretAccessKey) {
        throw new Error('AWS secret access key missing!')
    }
    if (!config.awsRegion) {
        throw new Error('AWS region missing!')
    }
    if (!config.applicationId) {
        throw new Error('ApplicationId missing!')
    }

    const uploadMegabytes = Math.max(1, Math.min(parseInt(config.uploadMegabytes) || 1, 100))
    const uploadSeconds = Math.max(1, Math.min(parseInt(config.uploadSeconds) || 1, 60))
    global.pinpoint = new PinpointClient({
        credentials: {
            accessKeyId: config.awsAccessKey,
            secretAccessKey: config.awsSecretAccessKey,
        },
        region: config.awsRegion,
        retryMode: 'standard',
        maxAttempts: config.maxAttempts  || 3,
    }) 

    global.buffer = createBuffer({
        limit: uploadMegabytes * 1024 * 1024,
        timeoutSeconds: uploadSeconds,
        onFlush: async (events) => {
            sendToPinpoint(events, meta)
        },
    })

    global.eventsToIgnore = new Set(
        config.eventsToIgnore ? config.eventsToIgnore.split(',').map((event) => event.trim()) : null
    )
}

export const teardownPlugin: PintpointPlugin['teardownPlugin'] = ({ global }) => {
    global.buffer.flush()
}

export const onEvent: PintpointPlugin['onEvent'] = (event, meta) => {
    let { global } = meta
    if (!global.eventsToIgnore.has(event.event)) {
        global.buffer.add(event)
    }
}

export const onSnapshot: PintpointPlugin['onSnapshot'] = async (event, { global }) => {
    if (!global.eventsToIgnore.has(event.event)) {
        if (!global.eventsToIgnore.has(event.event)) {
            global.buffer.add(event)
        }
    }
}

function sendToPinpoint(events: PluginEvent[], meta: PluginMeta<PintpointPlugin>) {
    let { config, global } = meta
    const command = new PutEventsCommand({
        ApplicationId: config.applicationId,
        EventsRequest: {
            BatchItem: events.reduce((batchEvents: { [key: string]: EventsBatch }, e) => {
                let batchKey = e.properties?.$device_id || Math.floor(Math.random() * 1000000).toString()
                batchEvents[batchKey] = {
                    Endpoint: fillEndpoint(e),
                    Events: { ...fillEvents([e]), ...batchEvents[batchKey].Events },
                }
                return batchEvents
            }, {}),
        },
    })

    global.pinpoint.send(command)
}

function fillEndpoint(event: PluginEvent) {
    let endpoint = {}
    if (event.properties?.$device_id) {
        endpoint = {
            Address: event.site_url,
            Attributes: {
                screen_density: event.properties?.$screen_density,
                screen_height: event.properties?.$screen_height,
                screen_name: event.properties?.$screen_name,
                screen_width: event.properties?.$screen_width,
                viewport_height: event.properties?.$viewport_height,
                viewport_width: event.properties?.$viewport_width,
            },
            ChannelType: event.properties?.$lib,
            Demographic: {
                AppVersion: event.properties?.$app_version,
                Locale: event.properties?.$locale,
                Make: event.properties?.$device_manufacturer || event.properties?.$device_type,
                Model: event.properties?.$device_model || event.properties?.$os,
                Platform: event.properties?.$os_name || event.properties?.$browser,
                PlatformVersion: event.properties?.$os_version || event.properties?.$browser_version,
                Timezone: event.properties?.$geoip_time_zone,
            },
            EffectiveDate: event.timestamp,
            Location: {
                City: event.properties?.$geoip_city_name,
                Country: event.properties?.$geoip_country_name,
                Latitude: event.properties?.$geoip_latitude,
                Longitude: event.properties?.$geoip_longitude,
                PostalCode: event.properties?.$geoip_postal_code,
                Region: event.properties?.$geoip_subdivision_1_code,
            },
            Metrics: {},
            RequestId: event.uuid,
            User: {
                UserId: event.properties?.$user_id,
            },
        }
    }
    return endpoint
}

function fillEvents(events: PluginEvent[]): { [key: string]: Event } {
    return events.reduce((pinpointEvents, event) => {
        pinpointEvents = {
            [event.distinct_id]: {
                AppPackageName: 'string;',
                AppTitle: 'string;',
                AppVersionCode: 'string;',
                Attributes: Object.keys(event.properties ?? {}).reduce((attributes, key) => {
                    attributes = {
                        [key]: JSON.stringify((event.properties ?? {})[key]),
                        ...attributes,
                    }
                    return attributes
                }, {}),
                ClientSdkVersion: event.properties?.$lib_version,
                EventType: event.event,
                Metrics: {
                    screen_density: event.properties?.$screen_density,
                    screen_height: event.properties?.$screen_height,
                    screen_name: event.properties?.$screen_name,
                    screen_width: event.properties?.$screen_width,
                    viewport_height: event.properties?.$viewport_height,
                    viewport_width: event.properties?.$viewport_width,
                },
                SdkName: event.properties?.$lib,
                Timestamp: event.timestamp,
                // Session: {
                //     Duration: 0,
                //     Id: 'string',
                //     StartTimestamp: 'string',
                //     StopTimestamp: ' string',
                // },
            },
        }
        return pinpointEvents
    }, {})
}
