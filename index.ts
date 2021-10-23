import { createBuffer } from '@posthog/plugin-contrib'
import { config, Pinpoint } from 'aws-sdk'
import { Plugin, PluginMeta, PluginEvent, Properties } from '@posthog/plugin-scaffold'
import { Event, PublicEndpoint, EventsBatch, PutEventsResponse } from 'aws-sdk/clients/pinpoint'
import { randomUUID } from 'crypto'

type PintpointPlugin = Plugin<{
    global: {
        pinpoint: Pinpoint
        buffer: ReturnType<typeof createBuffer>
        eventsToIgnore: Set<string>
    }
    config: {
        awsAccessKey: string
        awsSecretAccessKey: string
        awsRegion: string
        applicationId: string
        uploadSeconds: string
        uploadKilobytes: string
        eventsToIgnore: string
        maxAttempts: string
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

    const uploadKilobytes = Math.max(1, Math.min(parseInt(config.uploadKilobytes) || 1, 100))
    const uploadSeconds = Math.max(1, Math.min(parseInt(config.uploadSeconds) || 1, 60))
    const maxAttempts = parseInt(config.maxAttempts)
    global.pinpoint = new Pinpoint({
        credentials: {
            accessKeyId: config.awsAccessKey,
            secretAccessKey: config.awsSecretAccessKey,
        },
        region: config.awsRegion,
    })

    global.buffer = createBuffer({
        limit: uploadKilobytes * 1024 * 1024,
        timeoutSeconds: uploadSeconds,
        onFlush: async (events) => {
            console.info(`Buffer flushed :${events}`)
            if (events?.length) {
                sendToPinpoint(events, meta)
            }
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
        //global.buffer.add(event)
        sendToPinpoint([event], meta)
    }
}

export const onSnapshot: PintpointPlugin['onSnapshot'] = async (event, meta) => {
    let { global } = meta
    if (!global.eventsToIgnore.has(event.event)) {
        if (!global.eventsToIgnore.has(event.event)) {
            //global.buffer.add(event)
            sendToPinpoint([event], meta)
        }
    }
}

export const sendToPinpoint = async (events: PluginEvent[], meta: PluginMeta<PintpointPlugin>) => {
    let { config, global } = meta
    const command = {
        ApplicationId: config.applicationId,
        EventsRequest: {
            BatchItem: events.reduce((batchEvents: { [key: string]: EventsBatch }, e) => {
                let batchKey = e.properties?.$device_id || Math.floor(Math.random() * 1000000).toString()
                let pinpointEvents = getEvents([e])
                let pinpointEndpoint = getEndpoint(e)
                if (batchEvents[batchKey]) {
                    pinpointEvents = { ...pinpointEvents, ...batchEvents[batchKey].Events }
                }
                batchEvents[batchKey] = {
                    Endpoint: pinpointEndpoint,
                    Events: pinpointEvents,
                }
                return batchEvents
            }, {}),
        },
    }
    global.pinpoint.putEvents(command, (err: Error, data: PutEventsResponse) => {
        if (err) {
            console.error(`Error sending events to Pinpoint: ${err.message}:${JSON.stringify(command)}`)
            // if (payload.retriesPerformedSoFar >= 15) {
            //     return
            // }
            // const nextRetryMs = 2 ** payload.retriesPerformedSoFar * 3000
            // console.log(`Enqueued batch ${payload.batchId} for retry in ${nextRetryMs}ms`)
            // await jobs
            //     .uploadBatchToS3({
            //         ...payload,
            //         retriesPerformedSoFar: payload.retriesPerformedSoFar + 1,
            //     })
            //     .runIn(nextRetryMs, 'milliseconds')
        }
        console.info(
            `Uploaded ${events.length} event${events.length === 1 ? '' : 's'} to application ${config.applicationId}`
        )
        console.info(`Response: ${JSON.stringify(data)}`)
    })
}

export const getEndpoint = (event: PluginEvent): PublicEndpoint => {
    let endpoint = {}
    if (event.properties?.$device_id) {
        endpoint = {
            Address: event.site_url,
            Attributes: {
                screen_density: [event.properties?.$screen_density?.toString() || ''],
                screen_height: [event.properties?.$screen_height?.toString() || ''],
                screen_name: [event.properties?.$screen_name?.toString() || ''],
                screen_width: [event.properties?.$screen_width?.toString() || ''],
                viewport_height: [event.properties?.$viewport_height?.toString() || ''],
                viewport_width: [event.properties?.$viewport_width?.toString() || ''],
            },
            ChannelType: event.properties?.$lib,
            Demographic: {
                AppVersion: event.properties?.$app_version,
                Locale: event.properties?.$locale,
                Make: event.properties?.$device_manufacturer || event.properties?.$device_type,
                Model: event.properties?.$device_model || event.properties?.$os,
                Platform: event.properties?.$os_name || event.properties?.$browser,
                PlatformVersion:
                    event.properties?.$os_version?.toString() || event.properties?.$browser_version?.toString(),
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

export const getEvents = (events: PluginEvent[]): { [key: string]: Event } => {
    return events.reduce((pinpointEvents, event) => {
        console.info(`Event ${JSON.stringify(event)}`)
        let eventKey = event.uuid ?? randomUUID()
        let pinpointEvent = {
            AppPackageName: 'string;',
            AppTitle: 'string;',
            AppVersionCode: 'string;',
            Attributes: Object.keys(event.properties ?? {}).reduce((attributes, key) => {
                attributes = {
                    [key]: getAttribute(event, key),
                    ...attributes,
                }
                return attributes
            }, {}),
            ClientSdkVersion: event.properties?.$lib_version,
            EventType: event.event,
            Metrics: {},
            SdkName: event.properties?.$lib,
            Timestamp: event.timestamp || new Date().getTime().toString(),
            //Session: {},
            // Session: {
            //     Duration: 0,
            //     Id: 'string',
            //     StartTimestamp: 'string',
            //     StopTimestamp: ' string',
            // },
        }
        // if (event.properties?.$session_id) {
        //     pinpointEvent.Session = { Id: event.properties?.$session_id }
        // }
        pinpointEvents = {
            [eventKey]: pinpointEvent,
        }
        return pinpointEvents
    }, {})
}

export const getAttribute = (properties: Properties, key: string): string => {
    let value = (properties ?? {})[key]
    if (typeof value === 'number' || typeof value === 'boolean') {
        value = value.toString()
    } else if (typeof value !== 'string') {
        value = JSON.stringify(value)
    }
    return value
}
