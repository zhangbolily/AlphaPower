// routes/tabler-demo.tsx（或者直接放到你的某个页面组件里）
import { TablerScope } from '../components/TablerScope'

import {
    createFileRoute,
} from "@tanstack/react-router"

export const Route = createFileRoute("/tabler-demo")({
    component: TablerDemoPage,
})

export default function TablerDemoPage() {
    return (
        <TablerScope theme="light">
            <header className="navbar navbar-expand-md d-print-none">
                <div className="container-xl">
                    <div className="navbar-brand">My Admin</div>
                </div>
            </header>

            <div className="row row-cards">
                <div className="col-md-6">
                    <div className="row row-cards">
                        <div className="col-12">
                            {/* {% include "cards/users-list-2.html" %} */}
                        </div>
                        <div className="col-12">
                            {/* {% include "cards/users-list.html" offset=8 checkbox=true hover=true checked-ids="2,5,8" title="Contacts" %} */}
                        </div>
                        <div className="col-12">
                            <div className="card">
                                <div className="card-header">
                                    <h3 className="card-title">Links and buttons</h3>
                                </div>
                                <div className="list-group list-group-flush">
                                    <a href="#" className="list-group-item list-group-item-action active" aria-current="true">
                                        The current link item
                                    </a>
                                    <a href="#" className="list-group-item list-group-item-action">A second link item</a>
                                    <a href="#" className="list-group-item list-group-item-action">A third link item</a>
                                    <a href="#" className="list-group-item list-group-item-action">A fourth link item</a>
                                    <a className="list-group-item list-group-item-action disabled">A disabled link item</a>
                                </div>
                            </div>
                        </div>
                    </div>
                </div>
                <div className="col-md-6">
                    <div className="row row-cards">
                        <div className="col-12">
                            {/* {% include "cards/users-list.html" hoverable=true  %} */}
                        </div>
                        <div className="col-12">
                            {/* {% include "cards/users-list-headers.html" %} */}
                        </div>
                    </div>
                </div>
            </div>
        </TablerScope>
    )
}
