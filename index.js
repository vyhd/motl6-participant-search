class ParticipantClient {
    static BASE_URL = "https://ifxrn9gwff.execute-api.us-east-1.amazonaws.com/Prod"

    static LIST_PARTICIPANTS = `${ParticipantClient.BASE_URL}/participants`;
    static LIST_PARTICIPANT_EVENTS = `${ParticipantClient.BASE_URL}/events`;
    static LAST_UPDATE = `${ParticipantClient.BASE_URL}/last-update`;

    constructor(onUpdateParticipants, onEventFetch) {
        this.onUpdateParticipants = onUpdateParticipants;
        this.onEventFetch = onEventFetch;

        this.lastUpdate = localStorage.getItem("lastUpdate") ?? "never";
        this.participants = JSON.parse(localStorage.getItem("participants")) ?? [];
        this.lastEventFetch = JSON.parse(localStorage.getItem("lastEventFetch")) ?? null;
    }

    async forceUpdate() {
        localStorage.removeItem("lastUpdate");
        this.lastUpdate = "never";

        await this.update();
    }

    async call(url) {
        /* Calls a URL, returns its JSON, saves a bit of boilerplate. */
        const resp = await fetch(url);
        return resp.json();
    }

    async update() {
        console.debug("ParticipantClient.update");
        // We may have multiple sources, so check the last update time for each
        const lastUpdates = await this.call(ParticipantClient.LAST_UPDATE);
        let lastServerUpdate = Object.values(lastUpdates).join(",");

        if (this.lastUpdate !== lastServerUpdate) {
            console.debug(`Server version ${lastServerUpdate} mismatches local ${this.lastUpdate}. Updating.`)
            this.participants = []
            this.lastEventFetch = null;
        }

        if (this.participants.length === 0) {
            let resp = await this.call(ParticipantClient.LIST_PARTICIPANTS);
            this.participants = resp["names"];

            console.debug("participants fetch", resp);

            localStorage.setItem("participants", JSON.stringify(this.participants))
            this.onUpdateParticipants(this.participants);
        }
        
        if (this.lastEventFetch !== null) {
            this.onEventFetch(this.lastEventFetch);
        }

        localStorage.setItem("lastUpdate", lastServerUpdate);
    }

    async fetchEvents(name) {
        const url = `${ParticipantClient.LIST_PARTICIPANT_EVENTS}?name=${encodeURIComponent(name)}`;
        let resp = await this.call(url)
        console.debug("fetchEvents", resp);
        this.lastEventFetch = resp;
        localStorage.setItem("lastEventFetch", JSON.stringify(this.lastEventFetch));
        this.onEventFetch(this.lastEventFetch);
    }
}